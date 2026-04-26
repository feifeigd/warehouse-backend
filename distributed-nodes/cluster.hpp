#pragma once

#include "node_types.hpp"

struct node_config : actor_system_config {
  std::string name;
  std::string host = "127.0.0.1";
  std::string bind;
  uint16_t port;
  std::string master_host = "127.0.0.1";
  uint16_t master_port = 47000;
  std::string parent;
  std::string region = "region-a";
  std::string storage_key = "profile";
  uint32_t lease_seconds = 15;
  uint32_t node_heartbeat_seconds = 5;
  bool shutdown_parent_on_exit = false;
  bool shutdown_children_on_exit = false;
  uint32_t lifetime = 0;

  explicit node_config(std::string default_name, uint16_t default_port,
                       std::string default_parent = {},
                       std::string default_region = "region-a")
    : name(std::move(default_name)),
      port(default_port),
      parent(std::move(default_parent)),
      region(std::move(default_region)) {
    opt_group{custom_options_, "global"}
      .add(name, "name,n", "logical node name")
      .add(host, "host,H", "advertised host for the current node")
      .add(bind, "bind,B", "bind address for the local listener")
      .add(port, "port,p", "listener port for the current node")
      .add(master_host, "master-host", "master host for non-master nodes")
      .add(master_port, "master-port", "master port for non-master nodes")
      .add(parent, "parent", "parent node name for tree attachment")
      .add(region, "region,r", "target region for client mode")
      .add(storage_key, "storage-key", "lookup key for the storage node")
      .add(lease_seconds, "lease-seconds",
           "seconds before master/region evict a silent node")
      .add(node_heartbeat_seconds, "node-heartbeat-seconds",
           "seconds between node heartbeat refreshes, 0 disables heartbeats")
      .add(shutdown_parent_on_exit, "shutdown-parent-on-exit",
           "request parent shutdown when this node exits")
      .add(shutdown_children_on_exit, "shutdown-children-on-exit",
           "request child shutdown when this node exits")
      .add(lifetime, "lifetime,l",
           "node lifetime in seconds, 0 waits for <enter>");
  }
};

node_manifest make_manifest(const node_config& cfg, node_kind kind) {
  node_manifest manifest;
  manifest.kind = kind;
  manifest.node_name = cfg.name;
  manifest.host = cfg.host;
  manifest.port = cfg.port;
  manifest.parent = cfg.parent;
  manifest.exported_actors = exported_actor_names(kind);
  return manifest;
}

class cluster{
  actor_system& sys_;
  const node_config& cfg_;
  actor master_actor_;
public:
  cluster(actor_system& sys, const node_config& cfg, actor master_actor = {}) : sys_(sys), cfg_(cfg), master_actor_(master_actor) {
    // nop
  }

  bool connect_to_master() {
    master_actor_ = lookup_remote_named_actor(cfg_.master_host, cfg_.master_port,
                                            k_master_control);
    if (!master_actor_) {
      sys_.println("[{}] failed to lookup master actor at {}:{}", cfg_.name,
                  cfg_.master_host, cfg_.master_port);
      return false;
    }
    return true;
  }

  bool has_master_actor() const {
    return static_cast<bool>(master_actor_);
  }

  void mark_master_unavailable() {
    master_actor_ = {};
  }
  
  bool register_with_master(const node_manifest& manifest,
                            const actor& monitor_actor) {
    auto registered = with_retry(
      [&]() -> std::optional<bool> {
        if (!master_actor_ && !connect_to_master())
          return {};
        scoped_actor self{sys_};
        auto reply = request_register(self,
                                      node_registration{manifest, monitor_actor});
        if (!reply || !reply->ok)
          return {};
        sys_.println("[{}] registered with master: {}", cfg_.name,
                     reply->message);
        return true;
      },
      30s, 500ms);
    if (!registered) {
      sys_.println("[{}] registration was rejected", cfg_.name);
      return false;
    }
    return true;
  }

  actor lookup_remote_named_actor(const std::string& host,
                                  uint16_t port, const std::string& actor_name) {
    auto nid = with_retry([&] { return sys_.middleman().connect(host, port); });
    if (!nid)
      return {};
    auto actor_ptr = with_retry(
      [&] { return sys_.middleman().remote_lookup(actor_name, *nid); });
    if (!actor_ptr)
      return {};
    return actor_cast<actor>(actor_ptr);
  }

  bool attach_to_parent_region(const node_manifest& manifest,
                               const actor& monitor_actor) {
    if (manifest.parent.empty())
      return true;

    auto attached = with_retry(
      [&]() -> std::optional<bool> {
        if (!master_actor_ && !connect_to_master())
          return {};
        scoped_actor self{sys_};
        auto route = request_route(self, manifest.parent, k_region_router);
        if (!route) {
          sys_.println("[{}] could not resolve parent region '{}'",
                       manifest.node_name, manifest.parent);
          return {};
        }
        auto region_actor = lookup_remote_named_actor(route->host, route->port,
                                                      route->actor_name);
        if (!region_actor) {
          sys_.println("[{}] could not lookup parent region actor '{}'",
                       manifest.node_name, manifest.parent);
          return {};
        }
        auto ok = false;
        self->request(region_actor, 10s, region_attach_atom_v,
                      node_registration{manifest, monitor_actor}).receive(
          [&](const register_reply& reply) {
            sys_.println("[{}] parent attach: {}", manifest.node_name,
                         reply.message);
            ok = reply.ok;
          },
          [&](const error& err) {
            sys_.println("[{}] parent attach failed: {}", manifest.node_name,
                         to_string(err));
          }
        );
        return ok ? std::optional<bool>{true} : std::nullopt;
      }
    , 30s, 500ms);
    return static_cast<bool>(attached);
  }

  bool detach_from_parent_region(const node_manifest& manifest) {
    if (manifest.parent.empty())
      return true;
    if (!master_actor_)
      return false;
    scoped_actor self{sys_};
    auto route = request_route(self, manifest.parent, k_region_router);
    if (!route) {
      sys_.println("[{}] could not resolve parent region '{}' for detach",
                  manifest.node_name, manifest.parent);
      return false;
    }
    auto region_actor = lookup_remote_named_actor(route->host, route->port,
                                                  route->actor_name);
    if (!region_actor) {
      sys_.println("[{}] could not lookup parent region actor '{}' for detach",
                  manifest.node_name, manifest.parent);
      return false;
    }
    auto ok = false;
    self->request(region_actor, 10s, region_detach_atom_v, manifest.node_name)
      .receive(
        [&](const register_reply& reply) {
          sys_.println("[{}] parent detach: {}", manifest.node_name, reply.message);
          ok = reply.ok;
        },
        [&](const error& err) {
          sys_.println("[{}] parent detach failed: {}", manifest.node_name,
                      to_string(err));
        }
      );
    return ok;
  }

  bool unregister_from_master(const std::string& node_name) {
    if (!master_actor_)
      return false;
    scoped_actor self{sys_};
    auto ok = false;
    self->request(master_actor_, 10s, master_unregister_atom_v, node_name).receive(
      [&](const register_reply& reply) {
        sys_.println("[{}] unregister from master: {}", cfg_.name, reply.message);
        ok = reply.ok;
      },
      [&](const error& err) {
        sys_.println("[{}] unregister failed: {}", cfg_.name, to_string(err));
      }
    );
    return ok;
  }

  actor master_actor() const {
    return master_actor_;
  }

  actor lookup_parent_region_actor(const node_manifest& manifest) {
    if (manifest.parent.empty())
      return {};
    if (!master_actor_ && !connect_to_master())
      return {};
    scoped_actor self{sys_};
    auto route = request_route(self, manifest.parent, k_region_router);
    if (!route) {
      sys_.println("[{}] could not resolve parent region '{}' for heartbeat",
                   manifest.node_name, manifest.parent);
      return {};
    }
    auto region_actor = lookup_remote_named_actor(route->host, route->port,
                                                  route->actor_name);
    if (!region_actor) {
      sys_.println("[{}] could not lookup parent region actor '{}' for heartbeat",
                   manifest.node_name, manifest.parent);
      return {};
    }
    return region_actor;
  }

  bool heartbeat_master(const std::string& node_name) {
    if (!master_actor_ && !connect_to_master())
      return false;
    scoped_actor self{sys_};
    auto ok = false;
    self->request(master_actor_, 10s, master_heartbeat_atom_v, node_name).receive(
      [&](const register_reply& reply) {
        if (!reply.ok) {
          sys_.println("[{}] master heartbeat rejected: {}", node_name,
                       reply.message);
        }
        ok = reply.ok;
      },
      [&](const error& err) {
        sys_.println("[{}] master heartbeat failed: {}", node_name,
                     to_string(err));
        mark_master_unavailable();
      }
    );
    return ok;
  }

  bool heartbeat_parent_region(const node_manifest& manifest) {
    auto parent_actor = lookup_parent_region_actor(manifest);
    if (!parent_actor)
      return false;
    scoped_actor self{sys_};
    auto ok = false;
    self->request(parent_actor, 10s, region_heartbeat_atom_v,
                  manifest.node_name).receive(
      [&](const register_reply& reply) {
        if (!reply.ok) {
          sys_.println("[{}] parent heartbeat rejected: {}",
                       manifest.node_name, reply.message);
        }
        ok = reply.ok;
      },
      [&](const error& err) {
        sys_.println("[{}] parent heartbeat failed: {}", manifest.node_name,
                     to_string(err));
      }
    );
    return ok;
  }

  actor lookup_node_control_actor(scoped_actor& self,
                                  const std::string& node_name) {
    auto route = request_route(self, node_name, k_node_control);
    if (!route)
      return {};
    return lookup_remote_named_actor(route->host, route->port, route->actor_name);
  }

  bool request_node_shutdown(scoped_actor& self, const std::string& node_name,
                             const shutdown_request& request) {
    auto control = lookup_node_control_actor(self, node_name);
    if (!control) {
      sys_.println("[{}] could not lookup node control for '{}'", cfg_.name,
                   node_name);
      return false;
    }
    auto ok = false;
    self->request(control, 30s, node_shutdown_atom_v, request).receive(
      [&](const register_reply& reply) {
        ok = reply.ok;
      },
      [&](const error& err) {
        sys_.println("[{}] shutdown request to '{}' failed: {}", cfg_.name,
                     node_name, to_string(err));
      }
    );
    return ok;
  }

  std::optional<topology_snapshot> request_topology(scoped_actor& self) {
    if (!master_actor_)
      return {};
    std::optional<topology_snapshot> snapshot;
    self->request(master_actor_, 10s, master_topology_atom_v).receive(
      [&](const topology_snapshot& value) {
        snapshot = value;
      },
      [&](const error& err) {
        sys_.println("[{}] topology request failed: {}", cfg_.name, to_string(err));
        mark_master_unavailable();
      }
    );
    return snapshot;
  }
  
  std::optional<actor_route> request_route(scoped_actor& self,
                                          const std::string& node_name,
                                          const std::string& actor_name) {
    if (!master_actor_)
      return {};
    std::optional<actor_route> route;
    self->request(master_actor_, 10s, master_resolve_atom_v, node_name, actor_name)
      .receive(
        [&](const actor_route& value) {
          route = value;
        },
        [&](const error& err) {
          sys_.println("[{}] resolve {}:{} failed: {}", cfg_.name, node_name, actor_name,
                        to_string(err));
          if (err != sec::no_such_key)
            mark_master_unavailable();
        }
      );
    return route;
  }
  
  std::optional<child_snapshot> request_children(scoped_actor& self,
                                                const std::string& parent_name) {
    if (!master_actor_)
      return {};
    std::optional<child_snapshot> snapshot;
    self->request(master_actor_, 10s, master_children_atom_v, parent_name).receive(
      [&](const child_snapshot& value) {
        snapshot = value;
      },
      [&](const error& err) {
        self->println("children request failed: {}", to_string(err));
        mark_master_unavailable();
      }
    );
    return snapshot;
  }
private:

  std::optional<register_reply> request_register(scoped_actor& self,
                                                const node_registration& registration) {
    if (!master_actor_)
      return {};
    std::optional<register_reply> reply;
    self->request(master_actor_, 10s, master_register_atom_v, registration).receive(
      [&](const register_reply& value) {
        reply = value;
      },
      [&](const error& err) {
        self->println("registration failed: {}", to_string(err));
        mark_master_unavailable();
      }
    );
    return reply;
  }
};
