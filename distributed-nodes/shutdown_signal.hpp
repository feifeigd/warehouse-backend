#pragma once

#include <caf/actor_system.hpp>
#include <caf/response_promise.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

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
  static inline volatile std::sig_atomic_t requested_ = 0;
};

class shutdown_signal : public std::enable_shared_from_this<shutdown_signal> {
public:
  shutdown_signal() {
    process_shutdown_interrupt::install();
  }

  bool request_shutdown(shutdown_request request) {
    std::lock_guard<std::mutex> lock(mu_);
    if (request_)
      return false;
    request_ = std::move(request);
    cv_.notify_all();
    return true;
  }

  bool has_request() const {
    std::lock_guard<std::mutex> lock(mu_);
    return request_.has_value();
  }

  void add_completion_waiter(caf::response_promise waiter) {
    std::lock_guard<std::mutex> lock(mu_);
    if (completed_) {
      waiter.deliver(completion_reply_);
      return;
    }
    completion_waiters_.push_back(std::move(waiter));
  }

  void complete_shutdown(register_reply reply) {
    std::vector<caf::response_promise> waiters;
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (completed_)
        return;
      completed_ = true;
      completion_reply_ = std::move(reply);
      waiters = std::move(completion_waiters_);
    }
    for (auto& waiter : waiters)
      waiter.deliver(completion_reply_);
  }

  shutdown_request wait(caf::actor_system& sys, const std::string& role,
                        const std::string& node_name, uint32_t lifetime) {
    start_interrupt_watcher(node_name);
    if (lifetime > 0) {
      sys.println("[{}] running for {} seconds", role, lifetime);
      std::unique_lock<std::mutex> lock(mu_);
      if (!cv_.wait_for(lock, std::chrono::seconds{lifetime},
                        [&] { return request_.has_value(); })) {
        request_ = shutdown_request{
          node_name,
          node_name,
          "lifetime expired",
          shutdown_source::local,
        };
      }
      return *request_;
    }
    sys.println("[{}] press <enter> to stop", role);
    auto self = shared_from_this();
    std::thread([self, node_name] {
      std::string dummy;
      std::getline(std::cin, dummy);
      self->request_shutdown(shutdown_request{
        node_name,
        node_name,
        "console input",
        shutdown_source::local,
      });
    }).detach();
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [&] { return request_.has_value(); });
    return *request_;
  }

private:
  void start_interrupt_watcher(const std::string& node_name) {
    if (interrupt_watcher_started_.exchange(true))
      return;
    process_shutdown_interrupt::install();
    auto self = shared_from_this();
    std::thread([self, node_name] {
      while (!self->has_request()) {
        if (process_shutdown_interrupt::requested()) {
          self->request_shutdown(shutdown_request{
            node_name,
            node_name,
            "Ctrl+C",
            shutdown_source::external,
          });
          return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
      }
    }).detach();
  }

  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::optional<shutdown_request> request_;
  bool completed_ = false;
  register_reply completion_reply_{true, "shutdown complete"};
  std::vector<caf::response_promise> completion_waiters_;
  std::atomic_bool interrupt_watcher_started_ = false;
};
