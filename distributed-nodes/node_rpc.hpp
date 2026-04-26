#pragma once

#include "cluster.hpp"

struct rpc_client_state {
  event_based_actor* self;
  std::string master_host;
  uint16_t master_port = 0;
  actor master_actor;
  std::unordered_map<std::string, actor> actor_cache;

  rpc_client_state(event_based_actor* selfptr, std::string host, uint16_t port,
                   actor initial_master = {})
    : self(selfptr),
      master_host(std::move(host)),
      master_port(port),
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

  rpc_actor_result failure(std::string message) const {
    return rpc_actor_result{false, {}, std::move(message)};
  }

  rpc_actor_result success(actor remote, std::string message) const {
    return rpc_actor_result{true, std::move(remote), std::move(message)};
  }

  void resolve_remote_actor(const std::string& node_name,
                            const std::string& actor_name,
                            const std::string& key,
                            response_promise promise) {
    if (!ensure_master()) {
      promise.deliver(failure("master unavailable"));
      return;
    }
    self->request(master_actor, 10s, master_resolve_atom_v, node_name,
                  actor_name).then(
      [this, key, promise = std::move(promise)](const actor_route& route)
        mutable {
        auto remote = lookup_remote_actor(self->system(), route.host,
                                          route.port, route.actor_name, 0ms,
                                          0ms);
        if (!remote) {
          auto message = "service unavailable: " + route.node_name + "/"
                         + route.actor_name;
          self->println("[rpc] {}", message);
          promise.deliver(failure(std::move(message)));
          return;
        }
        actor_cache[key] = remote;
        promise.deliver(success(remote, "resolved"));
      },
      [this, node_name, actor_name, promise = std::move(promise)](
        const error& err) mutable {
        if (err != sec::no_such_key)
          master_actor = {};
        auto message = "service not registered: " + node_name + "/"
                       + actor_name + " (" + to_string(err) + ")";
        self->println("[rpc] {}", message);
        promise.deliver(failure(std::move(message)));
      }
    );
  }

  behavior make_behavior() {
    return {
      [this](rpc_resolve_actor_atom, const std::string& node_name,
             const std::string& actor_name) -> result<rpc_actor_result> {
        const auto key = cache_key(node_name, actor_name);
        if (auto cached = cached_actor(key))
          return success(cached, "cached");
        auto promise = self->make_response_promise();
        resolve_remote_actor(node_name, actor_name, key, promise);
        return promise;
      },
      [this](rpc_invalidate_actor_atom, const std::string& node_name,
             const std::string& actor_name) {
        actor_cache.erase(cache_key(node_name, actor_name));
        return register_reply{true, "rpc actor cache invalidated"};
      },
    };
  }
};

template <class T>
struct rpc_call_result {
  std::optional<T> value;
  std::string message;

  bool ok() const {
    return value.has_value();
  }
};

struct rpc_notify_result {
  bool ok = false;
  std::string message;
};

actor spawn_rpc_client(actor_system& sys, const node_config& cfg,
                       actor master_actor = {}) {
  return sys.spawn(actor_from_state<rpc_client_state>, cfg.master_host,
                   cfg.master_port, std::move(master_actor));
}

rpc_actor_result rpc_resolve_actor(scoped_actor& self, const actor& rpc_client,
                                   const std::string& node_name,
                                   const std::string& actor_name) {
  rpc_actor_result result;
  self->request(rpc_client, 10s, rpc_resolve_actor_atom_v, node_name,
                actor_name).receive(
    [&](const rpc_actor_result& value) {
      result = value;
    },
    [&](const error& err) {
      result = rpc_actor_result{false, {}, "rpc resolve failed: "
                                           + to_string(err)};
    }
  );
  return result;
}

void rpc_invalidate_actor(scoped_actor& self, const actor& rpc_client,
                          const std::string& node_name,
                          const std::string& actor_name) {
  self->request(rpc_client, 5s, rpc_invalidate_actor_atom_v, node_name,
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
  auto resolved = rpc_resolve_actor(self, rpc_client, node_name, actor_name);
  if (!resolved.ok)
    return rpc_notify_result{false, resolved.message};
  anon_send(resolved.remote, std::forward<Args>(args)...);
  return rpc_notify_result{true, "sent"};
}

template <class... Args>
rpc_call_result<register_reply> rpc_command(scoped_actor& self,
                                            const actor& rpc_client,
                                            const std::string& node_name,
                                            const std::string& actor_name,
                                            Args&&... args) {
  auto resolved = rpc_resolve_actor(self, rpc_client, node_name, actor_name);
  if (!resolved.ok)
    return {{}, resolved.message};

  rpc_call_result<register_reply> result;
  self->request(resolved.remote, 10s, std::forward<Args>(args)...).receive(
    [&](const register_reply& reply) {
      result.value = reply;
      result.message = reply.message;
    },
    [&](const error& err) {
      rpc_invalidate_actor(self, rpc_client, node_name, actor_name);
      result.message = "command failed: " + node_name + "/" + actor_name
                       + " (" + to_string(err) + ")";
    }
  );
  return result;
}

rpc_call_result<analytics_result> rpc_compute_analyze(
  scoped_actor& self, const actor& rpc_client, const std::string& node_name,
  const analytics_request& request) {
  auto resolved = rpc_resolve_actor(self, rpc_client, node_name,
                                    k_compute_service);
  if (!resolved.ok)
    return {{}, resolved.message};

  rpc_call_result<analytics_result> result;
  self->request(resolved.remote, 10s, compute_analyze_atom_v, request).receive(
    [&](const analytics_result& value) {
      result.value = value;
      result.message = "ok";
    },
    [&](const error& err) {
      rpc_invalidate_actor(self, rpc_client, node_name, k_compute_service);
      result.message = "compute service unavailable: " + node_name + " ("
                       + to_string(err) + ")";
    }
  );
  return result;
}

rpc_call_result<storage_result> rpc_storage_lookup(
  scoped_actor& self, const actor& rpc_client, const std::string& node_name,
  const storage_request& request) {
  auto resolved = rpc_resolve_actor(self, rpc_client, node_name,
                                    k_storage_service);
  if (!resolved.ok)
    return {{}, resolved.message};

  rpc_call_result<storage_result> result;
  self->request(resolved.remote, 10s, storage_lookup_atom_v, request).receive(
    [&](const storage_result& value) {
      result.value = value;
      result.message = "ok";
    },
    [&](const error& err) {
      rpc_invalidate_actor(self, rpc_client, node_name, k_storage_service);
      result.message = "storage service unavailable: " + node_name + " ("
                       + to_string(err) + ")";
    }
  );
  return result;
}
