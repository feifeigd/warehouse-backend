#pragma once

#include "node_types.hpp"

#include <caf/actor.hpp>
#include <caf/actor_from_state.hpp>
#include <caf/actor_system.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/response_promise.hpp>
#include <caf/scoped_actor.hpp>

#include <atomic>
#include <csignal>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

// Ctrl+C / SIGTERM 处理器，用于触发节点的优雅关机。
class process_shutdown_interrupt {
public:
  static void install() {
    std::call_once(install_once_, [] {
      std::signal(SIGINT, process_shutdown_interrupt::handle);
      std::signal(SIGTERM, process_shutdown_interrupt::handle);
    });
  }

  static bool requested() {
    return requested_ != 0;
  }

private:
  static void handle(int) {
    requested_ = 1;
  }

  static inline std::once_flag install_once_;
  static inline volatile std::sig_atomic_t requested_ = 0; // 收到了终止信号
};

// 只负责产生和保存关机请求，不负责“关机完成”的等待。
struct shutdown_signal_state {
  event_based_actor* self;
  std::string node_name;
  uint32_t lifetime = 0;
  std::optional<shutdown_request> request; // 收到的关机请求
  std::vector<response_promise> request_waiters; // 等待关机请求的响应者

  shutdown_signal_state(event_based_actor* selfptr, std::string node,
                        uint32_t lifetime_seconds)
    : self(selfptr),
      node_name(std::move(node)),
      lifetime(lifetime_seconds) {
    // nop
  }

  void schedule_poll() {
    self->delayed_send(self, 100ms, shutdown_signal_poll_atom_v);
  }

  void schedule_lifetime() {
    if (lifetime > 0)
      self->delayed_send(self, std::chrono::seconds{lifetime},
                         shutdown_signal_lifetime_atom_v);
  }

  register_reply submit(shutdown_request value) {
    if (request)
      return register_reply{true, "shutdown already pending"};
    request = std::move(value);
    auto waiters = std::move(request_waiters);
    request_waiters.clear();
    for (auto& waiter : waiters)
      waiter.deliver(*request);
    return register_reply{true, "shutdown requested"};
  }

  behavior make_behavior() {
    schedule_poll();
    schedule_lifetime();
    return {
      [this](shutdown_signal_request_atom, shutdown_request value) {
        return submit(std::move(value));
      },
      // 等待关机信号。
      [this](shutdown_signal_wait_atom) -> result<shutdown_request> {
        if (request)
          return *request;
        auto waiter = self->make_response_promise();
        request_waiters.push_back(waiter);
        return waiter;
      },
      // 定时轮询是否收到了 Ctrl+C / SIGTERM。
      [this](shutdown_signal_poll_atom) {
        if (!request && process_shutdown_interrupt::requested()) {
          submit(shutdown_request{
            node_name,
            node_name,
            "Ctrl+C",
            shutdown_source::external,
          });
          return;
        }
        if (!request)
          schedule_poll();
      },
      // 到达预设生命周期，自动提交关机请求。
      [this](shutdown_signal_lifetime_atom) {
        if (!request) {
          submit(shutdown_request{
            node_name,
            node_name,
            "lifetime expired",
            shutdown_source::local,
          });
        }
      },
    };
  }
};

class shutdown_signal {
public:
  shutdown_signal() {
    process_shutdown_interrupt::install();
  }

  actor start(actor_system& sys, const std::string& node_name,
              uint32_t lifetime) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!worker_)
      worker_ = sys.spawn(actor_from_state<shutdown_signal_state>, node_name,
                          lifetime);
    return worker_;
  }

  actor actor_handle() const {
    std::lock_guard<std::mutex> lock(mu_);
    return worker_;
  }

  void add_completion_waiter(response_promise waiter) {
    std::lock_guard<std::mutex> lock(completion_mu_);
    if (completed_) {
      waiter.deliver(completion_reply_);
      return;
    }
    completion_waiters_.push_back(std::move(waiter));
  }

  // 主线程调用，关机流程完成时回调等待者。
  void complete_shutdown(register_reply reply) {
    std::vector<response_promise> waiters;
    {
      std::lock_guard<std::mutex> lock(completion_mu_);
      if (completed_)
        return;
      completed_ = true;
      completion_reply_ = std::move(reply);
      waiters = std::move(completion_waiters_);
    }
    for (auto& waiter : waiters)
      waiter.deliver(completion_reply_);
  }

  shutdown_request wait(actor_system& sys, const std::string& role,
                        const std::string& node_name, uint32_t lifetime) {
    auto worker = start(sys, node_name, lifetime);
    if (lifetime > 0) {
      sys.println("[{}] running for {} seconds", role, lifetime);
    } else {
      sys.println("[{}] press Ctrl+C to stop", role);
    }

    // 主线程阻塞等待关机信号。后续可改成 update loop 轮询/驱动。
    scoped_actor self{sys};
    shutdown_request result;
    self->request(worker, infinite, shutdown_signal_wait_atom_v).receive(
      [&](const shutdown_request& value) {
        result = value;
      },
      [&](const error& err) {
        result = shutdown_request{
          node_name,
          node_name,
          "shutdown wait failed: " + to_string(err),
          shutdown_source::local,
        };
      }
    );
    return result;
  }

private:
  mutable std::mutex mu_;
  actor worker_;
  mutable std::mutex completion_mu_;
  bool completed_ = false;
  register_reply completion_reply_{true, "shutdown complete"};
  std::vector<response_promise> completion_waiters_;
};
