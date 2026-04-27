#pragma once

#include "node_types.hpp"
#include "node_config.hpp"

struct rpc_client_state {
  event_based_actor* self;
  std::string master_host;
  uint16_t master_port = 0;
  std::chrono::milliseconds resolve_timeout;
  actor master_actor;
  std::unordered_map<std::string, actor> actor_cache;
  std::map<actor_addr, std::set<std::string>> cache_keys_by_addr;
  std::unordered_map<std::string, std::vector<response_promise>>
    pending_resolutions;

  rpc_client_state(event_based_actor* selfptr, std::string host, uint16_t port,
                   std::chrono::milliseconds resolve_wait,
                   actor initial_master = {})
    : self(selfptr),
      master_host(std::move(host)),
      master_port(port),
      resolve_timeout(resolve_wait),
      master_actor(std::move(initial_master)) {
    // nop
  }

  std::string cache_key(const std::string& node_name,
                        const std::string& actor_name) const {
    return node_name + "/" + actor_name;
  }

  actor cached_actor(const std::string& key) const {
    auto iter = actor_cache.find(key);
    if (iter == actor_cache.end())
      return {};
    return iter->second;
  }

  void erase_cached_actor(const std::string& key) {
    auto iter = actor_cache.find(key);
    if (iter == actor_cache.end())
      return;
    auto remote = iter->second;
    auto addr = remote.address();
    actor_cache.erase(iter);
    auto reverse_iter = cache_keys_by_addr.find(addr);
    if (reverse_iter == cache_keys_by_addr.end())
      return;
    auto& keys = reverse_iter->second;
    keys.erase(key);
    if (!keys.empty())
      return;
    cache_keys_by_addr.erase(reverse_iter);
    self->demonitor(remote);
  }

  void cache_actor(const std::string& key, const actor& remote) {
    erase_cached_actor(key);
    actor_cache[key] = remote;
    auto& keys = cache_keys_by_addr[remote.address()];
    auto first_reference = keys.empty();
    keys.insert(key);
    if (first_reference)
      self->monitor(remote);
  }

  bool ensure_master() {
    if (master_actor)
      return true;
    master_actor = lookup_remote_actor(self->system(), master_host, master_port,
                                       k_master_control, 0ms, 0ms);
    if (!master_actor) {
      self->println("[rpc] master unavailable at {}:{}", master_host,
                    master_port);
      return false;
    }
    return true;
  }

  rpc_actor_result failure(rpc_error_code code, std::string message) const {
    return rpc_actor_result{false, {}, code, std::move(message)};
  }

  rpc_actor_result success(actor remote, std::string message) const {
    return rpc_actor_result{
      true,
      std::move(remote),
      rpc_error_code::none,
      std::move(message),
    };
  }

  bool queue_pending_resolution(const std::string& key,
                                response_promise promise) {
    auto [iter, inserted] = pending_resolutions.try_emplace(key);
    iter->second.push_back(std::move(promise));
    return inserted;
  }

  void finish_pending_resolution(const std::string& key,
                                 const rpc_actor_result& result) {
    auto iter = pending_resolutions.find(key);
    if (iter == pending_resolutions.end())
      return;
    auto waiters = std::move(iter->second);
    pending_resolutions.erase(iter);
    for (auto& waiter : waiters)
      waiter.deliver(result);
  }

  void resolve_remote_actor(const std::string& node_name,
                            const std::string& actor_name,
                            const std::string& key) {
    if (!ensure_master()) {
      finish_pending_resolution(key, failure(rpc_error_code::master_unavailable,
                                             "master unavailable"));
      return;
    }
    self->request(master_actor, resolve_timeout, master_resolve_atom_v,
                  node_name, actor_name).then(
      [this, key](const actor_route& route)
        mutable {
        auto remote = lookup_remote_actor(self->system(), route.host,
                                          route.port, route.actor_name, 0ms,
                                          0ms);
        if (!remote) {
          auto message = "service unavailable: " + route.node_name + "/"
                         + route.actor_name;
          self->println("[rpc] {}", message);
          finish_pending_resolution(key,
                                    failure(rpc_error_code::service_unavailable,
                                            std::move(message)));
          return;
        }
        cache_actor(key, remote);
        finish_pending_resolution(key, success(remote, "resolved"));
      },
      [this, key, node_name, actor_name](
        const error& err) mutable {
        if (err != sec::no_such_key)
          master_actor = {};
        auto code = err == sec::no_such_key
                    ? rpc_error_code::service_not_registered
                    : rpc_error_code::master_unavailable;
        auto message = "service not registered: " + node_name + "/"
                       + actor_name + " (" + to_string(err) + ")";
        self->println("[rpc] {}", message);
        finish_pending_resolution(key, failure(code, std::move(message)));
      }
    );
  }

  behavior make_behavior() {
    return {
      // 查找远程actor的句柄，先从本地缓存查找，如果没有则请求master进行解析。
      // 成功时将结果缓存起来，并监视该actor以便及时清理缓存；失败时返回相应的错误码和消息。
      [this](rpc_resolve_actor_atom, const std::string& node_name,
             const std::string& actor_name) -> result<rpc_actor_result> {
        const auto key = cache_key(node_name, actor_name);
        if (auto cached = cached_actor(key))
          return success(cached, "cached");
        auto promise = self->make_response_promise();
        if (queue_pending_resolution(key, promise))
          resolve_remote_actor(node_name, actor_name, key);
        return promise;
      },
      [this](rpc_invalidate_actor_atom, const std::string& node_name,
             const std::string& actor_name) {
        erase_cached_actor(cache_key(node_name, actor_name));
        return register_reply{true, "rpc actor cache invalidated"};
      },
      [this](const down_msg& msg) {
        auto iter = cache_keys_by_addr.find(msg.source);
        if (iter == cache_keys_by_addr.end())
          return;
        auto keys = std::move(iter->second);
        cache_keys_by_addr.erase(iter);
        for (const auto& key : keys) {
          self->println("[rpc] cached actor '{}' went down: {}", key,
                        to_string(msg.reason));
          actor_cache.erase(key);
        }
      },
    };
  }
};

template <class T>
struct rpc_call_result {
  std::optional<T> value;
  rpc_error_code code = rpc_error_code::none;
  std::string message;

  bool ok() const {
    return value.has_value();
  }
};

struct rpc_notify_result {
  bool ok = false;
  rpc_error_code code = rpc_error_code::none;
  std::string message;
};

struct rpc_timeout_options {
  std::chrono::milliseconds resolve = 10000ms;
  std::chrono::milliseconds request = 10000ms;
  std::chrono::milliseconds invalidate = 5000ms;
};

inline rpc_timeout_options make_rpc_timeout_options(const node_config& cfg) {
  return rpc_timeout_options{
    cfg.rpc_resolve_timeout(),
    cfg.rpc_request_timeout(),
    cfg.rpc_invalidate_timeout(),
  };
}

template <class... Args>
rpc_notify_result rpc_notify(scoped_actor& self, const actor& rpc_client,
                             const rpc_timeout_options& timeouts,
                             const std::string& node_name,
                             const std::string& actor_name,
                             Args&&... args);

template <class... Args>
rpc_call_result<register_reply> rpc_command(scoped_actor& self,
                                            const actor& rpc_client,
                                            const rpc_timeout_options& timeouts,
                                            const std::string& node_name,
                                            const std::string& actor_name,
                                            Args&&... args);

inline rpc_call_result<analytics_result> rpc_compute_analyze(
  scoped_actor& self, const actor& rpc_client,
  const rpc_timeout_options& timeouts, const std::string& node_name,
  const analytics_request& request);

inline rpc_call_result<storage_result> rpc_storage_lookup(
  scoped_actor& self, const actor& rpc_client,
  const rpc_timeout_options& timeouts, const std::string& node_name,
  const storage_request& request);

inline actor spawn_rpc_client(actor_system& sys, const node_config& cfg,
                              actor master_actor = {}) {
  return sys.spawn(actor_from_state<rpc_client_state>, cfg.master_host,
                   cfg.master_port, cfg.rpc_resolve_timeout(),
                   std::move(master_actor));
}

inline rpc_actor_result rpc_resolve_actor(
  scoped_actor& self, const actor& rpc_client, const std::string& node_name,
  const std::string& actor_name,
  std::chrono::milliseconds timeout = 10000ms) {
  rpc_actor_result result;
  self->request(rpc_client, timeout, rpc_resolve_actor_atom_v, node_name,
                actor_name).receive(
    [&](const rpc_actor_result& value) {
      result = value;
    },
    [&](const error& err) {
      result = rpc_actor_result{
        false,
        {},
        rpc_error_code::resolve_failed,
        "rpc resolve failed: " + to_string(err),
      };
    }
  );
  return result;
}

inline void rpc_invalidate_actor(scoped_actor& self, const actor& rpc_client,
                                 const std::string& node_name,
                                 const std::string& actor_name,
                                 std::chrono::milliseconds timeout = 5000ms) {
  self->request(rpc_client, timeout, rpc_invalidate_actor_atom_v, node_name,
                actor_name).receive(
    [](const register_reply&) {
      // nop
    },
    [](const error&) {
      // nop
    }
  );
}

template <class... Args>
rpc_notify_result rpc_notify(scoped_actor& self, const actor& rpc_client,
                             const std::string& node_name,
                             const std::string& actor_name,
                             Args&&... args) {
  return rpc_notify(self, rpc_client, rpc_timeout_options{}, node_name,
                    actor_name, std::forward<Args>(args)...);
}

template <class... Args>
rpc_notify_result rpc_notify(scoped_actor& self, const actor& rpc_client,
                             const rpc_timeout_options& timeouts,
                             const std::string& node_name,
                             const std::string& actor_name,
                             Args&&... args) {
  auto resolved = rpc_resolve_actor(self, rpc_client, node_name, actor_name,
                                    timeouts.resolve);
  if (!resolved.ok)
    return rpc_notify_result{false, resolved.code, resolved.message};
  anon_send(resolved.remote, std::forward<Args>(args)...);
  return rpc_notify_result{true, rpc_error_code::none, "sent"};
}

inline rpc_error_code rpc_request_error_code(const error& err) {
  return err == sec::request_timeout ? rpc_error_code::request_timeout
                                     : rpc_error_code::request_failed;
}

template <class Result, class... Args>
rpc_call_result<Result> rpc_request(scoped_actor& self,
                                    const actor& rpc_client,
                                    const rpc_timeout_options& timeouts,
                                    const std::string& node_name,
                                    const std::string& actor_name,
                                    const std::string& failure_context,
                                    Args&&... args) {
  auto resolved = rpc_resolve_actor(self, rpc_client, node_name, actor_name,
                                    timeouts.resolve);
  if (!resolved.ok)
    return {{}, resolved.code, resolved.message};

  rpc_call_result<Result> result;
  self->request(resolved.remote, timeouts.request,
                std::forward<Args>(args)...).receive(
    [&](const Result& value) {
      result.value = value;
      result.code = rpc_error_code::none;
      result.message = "ok";
    },
    [&](const error& err) {
      rpc_invalidate_actor(self, rpc_client, node_name, actor_name,
                           timeouts.invalidate);
      result.code = rpc_request_error_code(err);
      result.message = failure_context + ": " + node_name + "/" + actor_name
                       + " (" + to_string(err) + ")";
    }
  );
  return result;
}

template <class... Args>
rpc_call_result<register_reply> rpc_command(scoped_actor& self,
                                            const actor& rpc_client,
                                            const std::string& node_name,
                                            const std::string& actor_name,
                                            Args&&... args) {
  return rpc_command(self, rpc_client, rpc_timeout_options{}, node_name,
                     actor_name, std::forward<Args>(args)...);
}

template <class... Args>
rpc_call_result<register_reply> rpc_command(scoped_actor& self,
                                            const actor& rpc_client,
                                            const rpc_timeout_options& timeouts,
                                            const std::string& node_name,
                                            const std::string& actor_name,
                                            Args&&... args) {
  auto result = rpc_request<register_reply>(
    self, rpc_client, timeouts, node_name, actor_name, "command failed",
    std::forward<Args>(args)...);
  if (result.value)
    result.message = result.value->message;
  return result;
}

inline rpc_call_result<analytics_result> rpc_compute_analyze(
  scoped_actor& self, const actor& rpc_client, const std::string& node_name,
  const analytics_request& request) {
  return rpc_compute_analyze(self, rpc_client, rpc_timeout_options{}, node_name,
                             request);
}

inline rpc_call_result<analytics_result> rpc_compute_analyze(
  scoped_actor& self, const actor& rpc_client,
  const rpc_timeout_options& timeouts, const std::string& node_name,
  const analytics_request& request) {
  return rpc_request<analytics_result>(
    self, rpc_client, timeouts, node_name, k_compute_service,
    "compute service unavailable", compute_analyze_atom_v, request
  );
}

inline rpc_call_result<storage_result> rpc_storage_lookup(
  scoped_actor& self, const actor& rpc_client, const std::string& node_name,
  const storage_request& request) {
  return rpc_storage_lookup(self, rpc_client, rpc_timeout_options{}, node_name,
                            request);
}

inline rpc_call_result<storage_result> rpc_storage_lookup(
  scoped_actor& self, const actor& rpc_client,
  const rpc_timeout_options& timeouts, const std::string& node_name,
  const storage_request& request) {
  return rpc_request<storage_result>(
    self, rpc_client, timeouts, node_name, k_storage_service,
    "storage service unavailable", storage_lookup_atom_v, request
  );
}
