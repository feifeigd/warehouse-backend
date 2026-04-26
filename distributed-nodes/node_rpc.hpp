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
      return true;  // 不保证没失效啊
    master_actor = lookup_remote_actor(self->system(), master_host, master_port,
                                       k_master_control, 0ms, 0ms);
    if (!master_actor) {
      self->println("[rpc] failed to connect master at {}:{}", master_host,
                    master_port);
      return false;
    }
    return true;
  }

  void resolve_remote_actor(const std::string& node_name,
                            const std::string& actor_name,
                            const std::string& key,
                            response_promise promise) {
    if (!ensure_master()) {
      promise.deliver(actor{});
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
          self->println("[rpc] failed to lookup {}:{} at {}:{}",
                        route.node_name, route.actor_name, route.host,
                        route.port);
          promise.deliver(actor{});
          return;
        }
        actor_cache[key] = remote;
        promise.deliver(remote);
      },
      [this, promise = std::move(promise)](const error& err) mutable {
        self->println("[rpc] resolve failed: {}", to_string(err));
        if (err != sec::no_such_key)
          master_actor = {};
        promise.deliver(actor{});
      }
    );
  }

  behavior make_behavior() {
    return {
      // 注意：master控制器不应该频繁调用这个接口，最好是有个本地缓存，定期刷新
      [this](rpc_resolve_actor_atom, const std::string& node_name,
             const std::string& actor_name) -> result<actor> {
        const auto key = cache_key(node_name, actor_name);
        if (auto cached = cached_actor(key))
          return cached;
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

actor spawn_rpc_client(actor_system& sys, const node_config& cfg,
                       actor master_actor = {}) {
  return sys.spawn(actor_from_state<rpc_client_state>, cfg.master_host,
                   cfg.master_port, std::move(master_actor));
}

actor rpc_resolve_actor(scoped_actor& self, const actor& rpc_client,
                        const std::string& node_name,
                        const std::string& actor_name) {
  actor remote;
  self->request(rpc_client, 10s, rpc_resolve_actor_atom_v, node_name,
                actor_name).receive(
    [&](const actor& value) {
      remote = value;
    },
    [&](const error&) {
      remote = {};
    }
  );
  return remote;
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

std::optional<analytics_result> rpc_compute_analyze(
  scoped_actor& self, const actor& rpc_client, const std::string& node_name,
  analytics_request request) {
  auto remote = rpc_resolve_actor(self, rpc_client, node_name,
                                  k_compute_service);
  if (!remote)
    return {};
  std::optional<analytics_result> result;
  self->request(remote, 10s, compute_analyze_atom_v, std::move(request))
    .receive(
      [&](const analytics_result& value) {
        result = value;
      },
      [&](const error&) {
        rpc_invalidate_actor(self, rpc_client, node_name, k_compute_service);
        result = {};
      }
    );
  return result;
}

std::optional<storage_result> rpc_storage_lookup(
  scoped_actor& self, const actor& rpc_client, const std::string& node_name,
  storage_request request) {
  auto remote = rpc_resolve_actor(self, rpc_client, node_name,
                                  k_storage_service);
  if (!remote)
    return {};
  std::optional<storage_result> result;
  self->request(remote, 10s, storage_lookup_atom_v, std::move(request))
    .receive(
      [&](const storage_result& value) {
        result = value;
      },
      [&](const error&) {
        rpc_invalidate_actor(self, rpc_client, node_name, k_storage_service);
        result = {};
      }
    );
  return result;
}
