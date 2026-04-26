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
#include <iostream>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

// Ctrl+C handler for graceful shutdown of nodes.
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
  static inline volatile std::sig_atomic_t requested_ = 0;  // 收到了终止信号
};

struct shutdown_signal_state {
  event_based_actor* self;
  std::string node_name;
  uint32_t lifetime = 0;
  std::optional<shutdown_request> request;  // 收到的关机请求
  bool completed = false;
  register_reply completion_reply{true, "shutdown complete"};
  std::vector<response_promise> request_waiters;  // 等待关机请求的响应者
  std::vector<response_promise> completion_waiters; // 等待关机完成的响应者

  shutdown_signal_state(event_based_actor* selfptr, std::string node,
                        uint32_t lifetime_seconds)
    : self(selfptr),
      node_name(std::move(node)),
      lifetime(lifetime_seconds) {
    // nop
  }

  // 定时器轮询是否收到了关机请求
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

  void complete(register_reply reply) {
    if (completed)
      return;
    completed = true;
    completion_reply = std::move(reply);
    auto waiters = std::move(completion_waiters);
    completion_waiters.clear();
    for (auto& waiter : waiters)
      waiter.deliver(completion_reply);
  }

  behavior make_behavior() {
    schedule_poll();
    schedule_lifetime();
    return {
      [this](shutdown_signal_request_atom, shutdown_request value) {
        return submit(std::move(value));
      },
      // 等待关机信号
      [this](shutdown_signal_wait_atom) -> result<shutdown_request> {
        if (request)
          return *request;
        auto waiter = self->make_response_promise();
        request_waiters.push_back(waiter);
        return waiter;
      },
      [this](shutdown_signal_complete_atom, register_reply reply) {
        complete(std::move(reply));
        return register_reply{true, "shutdown completion delivered"};
      },
      // 等待关机完成
      [this](shutdown_signal_completion_wait_atom) -> result<register_reply> {
        if (completed)
          return completion_reply;
        auto waiter = self->make_response_promise();
        completion_waiters.push_back(waiter);
        return waiter;
      },
      // 定时器轮询是否收到了关机请求
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
          schedule_poll();  // 继续轮询
      },
      // 定时器触发，表示达到了预设的生命周期，自动提交关机请求
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

  // 主线程调用，关机完成时回调
  void complete_shutdown(register_reply reply) {
    auto worker = actor_handle();
    if (worker)
      anon_send(worker, shutdown_signal_complete_atom_v, reply);
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
      sys.println("[{}] press <enter> to stop", role);
      std::thread([worker, node_name] {
        std::string dummy;
        std::getline(std::cin, dummy);
        anon_send(worker, shutdown_signal_request_atom_v, shutdown_request{
          node_name,
          node_name,
          "console input",
          shutdown_source::local,
        });
      }).detach();
    }

    // 主线程阻塞等待关机信号
    scoped_actor self{sys};
    shutdown_request result;
    self->request(worker, infinite, shutdown_signal_wait_atom_v).receive(
      [&](const shutdown_request& value) {
        // 主线程，收到了关机请求
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
