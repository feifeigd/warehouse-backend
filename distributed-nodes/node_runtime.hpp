#pragma once

#include "node_types.hpp"
#include "shutdown_signal.hpp"

behavior node_control_actor_fun(event_based_actor* self, node_manifest manifest,
                                std::shared_ptr<shutdown_signal> signal) {
  return {
    [manifest](node_describe_atom) {
      return manifest;  // 
    },
    [self, manifest, signal](node_shutdown_atom,
                             shutdown_request request) -> result<register_reply> {
      const auto source = request.source;
      auto source_node = request.source_node.empty() ? "<unknown>"
                                                     : request.source_node;
      auto reason = request.reason;
      auto accepted = signal->request_shutdown(std::move(request));
      self->println("[{}:{}] shutdown {} from '{}' ({}) reason={}",
                    to_string(manifest.kind), manifest.node_name,
                    accepted ? "requested" : "already pending",
                    source_node, to_string(source), reason);
      if (source == shutdown_source::parent) {
        auto waiter = self->make_response_promise();
        signal->add_completion_waiter(waiter);
        return waiter;
      }
      return register_reply{true, accepted ? "shutdown requested"
                                           : "shutdown already pending"};
    },
  };
}


template <class F>
auto with_retry(F fn, std::chrono::milliseconds total = 4s,
                std::chrono::milliseconds step = 100ms) {
  auto result = fn();
  auto waited = 0ms;
  while (!result && waited < total) {
    std::this_thread::sleep_for(step);
    waited += step;
    result = fn();
  }
  return result;
}


bool open_node_port(actor_system& sys, const std::string& bind_addr,
                    uint16_t port) {
  const char* listen_addr = bind_addr.empty() ? nullptr : bind_addr.c_str();
  auto actual = sys.middleman().open(port, listen_addr, true);
  if (!actual) {
    sys.println("failed to open port {}: {}", port, to_string(actual.error()));
    return false;
  }
  return true;
}

void print_tree(actor_system& sys, const topology_snapshot& snapshot,
                const std::string& parent, int depth = 0) {
  std::vector<node_manifest> children;
  for (const auto& node : snapshot.nodes) {
    if (node.parent == parent)
      children.push_back(node);
  }
  sort_manifests(children);
  for (const auto& node : children) {
    auto indent = std::string(static_cast<size_t>(depth) * 2, ' ');
    sys.println("{}- {} [{}] {}:{} actors=[{}]", indent, node.node_name,
                to_string(node.kind), node.host, node.port,
                join_strings(node.exported_actors));
    print_tree(sys, snapshot, node.node_name, depth + 1);
  }
}


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
  
  bool register_with_master(const node_manifest& manifest,
                            const actor& monitor_actor) {
    if(!master_actor_) {
      if(!connect_to_master())
        return false;
    }

    scoped_actor self{sys_};
    auto reply = request_register(self, node_registration{manifest, monitor_actor});
    if (!reply || !reply->ok) {
      sys_.println("[{}] registration was rejected", cfg_.name);
      return false;
    }
    sys_.println("[{}] registered with master: {}", cfg_.name, reply->message);
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
    if (!master_actor_ && !connect_to_master())
      return false;
    scoped_actor self{sys_};
    auto route = request_route(self, manifest.parent, k_region_router);
    if (!route) {
      sys_.println("[{}] could not resolve parent region '{}'", manifest.node_name,
                  manifest.parent);
      return false;
    }
    auto region_actor = lookup_remote_named_actor(route->host, route->port,
                                                  route->actor_name);
    if (!region_actor) {
      sys_.println("[{}] could not lookup parent region actor '{}'",
                  manifest.node_name, manifest.parent);
      return false;
    }
    auto ok = false;
    self->request(region_actor, 10s, region_attach_atom_v,
                  node_registration{manifest, monitor_actor}).receive(
      [&](const register_reply& reply) {
        sys_.println("[{}] parent attach: {}", manifest.node_name, reply.message);
        ok = reply.ok;
      },
      [&](const error& err) {
        sys_.println("[{}] parent attach failed: {}", manifest.node_name,
                    to_string(err));
      }
    );
    return ok;
  }

  bool detach_from_parent_region(const node_manifest& manifest) {
    if (manifest.parent.empty())
      return true;
    if (!master_actor_ && !connect_to_master())
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
    if (!master_actor_ && !connect_to_master())
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
    std::optional<topology_snapshot> snapshot;
    self->request(master_actor_, 10s, master_topology_atom_v).receive(
      [&](const topology_snapshot& value) {
        snapshot = value;
      },
      [&](const error& err) {
        sys_.println("[{}] topology request failed: {}", cfg_.name, to_string(err));
      }
    );
    return snapshot;
  }
  
  std::optional<actor_route> request_route(scoped_actor& self,
                                          const std::string& node_name,
                                          const std::string& actor_name) {
    std::optional<actor_route> route;
    self->request(master_actor_, 10s, master_resolve_atom_v, node_name, actor_name)
      .receive(
        [&](const actor_route& value) {
          route = value;
        },
        [&](const error& err) {
          sys_.println("[{}] resolve {}:{} failed: {}", cfg_.name, node_name, actor_name,
                        to_string(err));
        }
      );
    return route;
  }
  
  std::optional<child_snapshot> request_children(scoped_actor& self,
                                                const std::string& parent_name) {
    std::optional<child_snapshot> snapshot;
    self->request(master_actor_, 10s, master_children_atom_v, parent_name).receive(
      [&](const child_snapshot& value) {
        snapshot = value;
      },
      [&](const error& err) {
        self->println("children request failed: {}", to_string(err));
      }
    );
    return snapshot;
  }
private:

  std::optional<register_reply> request_register(scoped_actor& self,
                                                const node_registration& registration) {
    std::optional<register_reply> reply;
    self->request(master_actor_, 10s, master_register_atom_v, registration).receive(
      [&](const register_reply& value) {
        reply = value;
      },
      [&](const error& err) {
        self->println("registration failed: {}", to_string(err));
      }
    );
    return reply;
  }
};

behavior node_heartbeat_actor_fun(event_based_actor* self, actor master_actor,
                                  actor parent_actor, std::string node_name,
                                  std::chrono::seconds interval) {
  auto schedule_next = [self, interval] {
    self->delayed_send(self, interval, heartbeat_tick_atom_v);
  };
  schedule_next();
  return {
    [=](heartbeat_tick_atom) mutable {
      self->request(master_actor, 10s, master_heartbeat_atom_v, node_name).then(
        [=](const register_reply& reply) mutable {
          if (!reply.ok) {
            self->println("[{}] master heartbeat rejected: {}", node_name,
                          reply.message);
            schedule_next();
            return;
          }
          if (!parent_actor) {
            schedule_next();
            return;
          }
          self->request(parent_actor, 10s, region_heartbeat_atom_v, node_name)
            .then(
              [=](const register_reply& parent_reply) mutable {
                if (!parent_reply.ok) {
                  self->println("[{}] parent heartbeat rejected: {}", node_name,
                                parent_reply.message);
                }
                schedule_next();
              },
              [=](const error& err) mutable {
                self->println("[{}] parent heartbeat failed: {}", node_name,
                              to_string(err));
                schedule_next();
              }
            );
        },
        [=](const error& err) mutable {
          self->println("[{}] master heartbeat failed: {}", node_name,
                        to_string(err));
          schedule_next();
        }
      );
    },
  };
}

/// Sends periodic heartbeats to the parent region and master.
class node_heartbeat {
public:
  node_heartbeat() = default;

  ~node_heartbeat() {
    stop();
  }

  bool start(actor_system& sys, cluster& sys_cluster, const node_config& cfg,
             const node_manifest& manifest, bool heartbeat_parent) {
    stop();
    if (cfg.node_heartbeat_seconds == 0)
      return true; // Heartbeats disabled by config.
    auto master_actor = sys_cluster.master_actor();
    if (!master_actor) {
      if (!sys_cluster.connect_to_master())
        return false;
      master_actor = sys_cluster.master_actor();
    }
    actor parent_actor;
    if (heartbeat_parent) {
      parent_actor = sys_cluster.lookup_parent_region_actor(manifest);
      if (!parent_actor)
        return false;
    }
    worker_ = sys.spawn(node_heartbeat_actor_fun, master_actor, parent_actor,
                        manifest.node_name,
                        std::chrono::seconds{cfg.node_heartbeat_seconds});
    return static_cast<bool>(worker_);
  }

  void stop() {
    if (worker_) {
      anon_send_exit(worker_, exit_reason::user_shutdown);
      worker_ = {};
    }
  }

private:
  actor worker_;
};

shutdown_request make_local_shutdown_request(const node_manifest& manifest,
                                             const std::string& reason) {
  return shutdown_request{
    manifest.node_name,
    manifest.node_name,
    reason,
    shutdown_source::local,
  };
}

shutdown_request make_parent_shutdown_request(const node_manifest& manifest,
                                              const shutdown_request& trigger) {
  return shutdown_request{
    trigger.initiator.empty() ? manifest.node_name : trigger.initiator,
    manifest.node_name,
    trigger.reason.empty() ? "parent cascade" : trigger.reason,
    shutdown_source::child,
  };
}

shutdown_request make_child_shutdown_request(const node_manifest& manifest,
                                             const shutdown_request& trigger) {
  return shutdown_request{
    trigger.initiator.empty() ? manifest.node_name : trigger.initiator,
    manifest.node_name,
    trigger.reason.empty() ? "child cascade" : trigger.reason,
    shutdown_source::parent,
  };
}

std::vector<std::string> collect_subtree_node_names(
  const topology_snapshot& snapshot, const std::string& root_name) {
  std::vector<std::string> result;
  std::vector<std::string> pending{root_name};
  for (size_t index = 0; index < pending.size(); ++index) {
    const auto& parent = pending[index];
    for (const auto& node : snapshot.nodes) {
      if (node.parent == parent) {
        result.push_back(node.node_name);
        pending.push_back(node.node_name);
      }
    }
  }
  return result;
}

bool topology_contains_any(const topology_snapshot& snapshot,
                           const std::vector<std::string>& node_names) {
  return std::any_of(snapshot.nodes.begin(), snapshot.nodes.end(),
                     [&](const node_manifest& node) {
                       return std::find(node_names.begin(), node_names.end(),
                                        node.node_name) != node_names.end();
                     });
}

void wait_for_subtree_shutdown(actor_system& sys, cluster& sys_cluster,
                               const node_config& cfg,
                               const node_manifest& manifest,
                               const std::vector<std::string>& node_names) {
  if (node_names.empty())
    return;
  scoped_actor self{sys};
  const auto deadline = steady_clock_type::now() + 15s;
  while (steady_clock_type::now() < deadline) {
    auto topology = sys_cluster.request_topology(self);
    if (!topology)
      break;
    if (!topology_contains_any(*topology, node_names)) {
      sys.println("[{}] subtree under '{}' stopped", cfg.name,
                  manifest.node_name);
      return;
    }
    std::this_thread::sleep_for(200ms);
  }
  sys.println("[{}] timed out waiting for subtree under '{}' to stop",
              cfg.name, manifest.node_name);
}

std::vector<std::string> propagate_shutdown_to_children(
                                    actor_system& sys, cluster& sys_cluster,
                                    const node_config& cfg,
                                    const node_manifest& manifest,
                                    const shutdown_request& trigger) {
  if (!cfg.shutdown_children_on_exit)
    return {};
  scoped_actor self{sys};
  auto topology = sys_cluster.request_topology(self);
  if (!topology)
    return {};
  auto subtree_node_names = collect_subtree_node_names(*topology,
                                                       manifest.node_name);
  auto request = make_child_shutdown_request(manifest, trigger);
  auto skip_child = trigger.source == shutdown_source::child
                    ? trigger.source_node
                    : std::string{};
  for (const auto& child : topology->nodes) {
    if (child.parent != manifest.node_name)
      continue;
    if (!skip_child.empty() && child.node_name == skip_child)
      continue;
    sys_cluster.request_node_shutdown(self, child.node_name, request);
  }
  return subtree_node_names;
}

void propagate_shutdown_to_parent(actor_system& sys, cluster& sys_cluster,
                                  const node_config& cfg,
                                  const node_manifest& manifest,
                                  const shutdown_request& trigger) {
  if (!cfg.shutdown_parent_on_exit || manifest.parent.empty())
    return;
  if (trigger.source == shutdown_source::parent)
    return;
  scoped_actor self{sys};
  sys_cluster.request_node_shutdown(self, manifest.parent,
                                    make_parent_shutdown_request(manifest,
                                                                 trigger));
}

// 椤哄簭鍏虫満锛氬厛閫氱煡瀛愯妭鐐癸紝鍐嶉€氱煡鐖惰妭鐐癸紝閬垮厤鐖惰妭鐐瑰厛鍏虫満瀵艰嚧瀛愯妭鐐规棤娉曟帴鏀跺叧鏈洪€氱煡銆?
void propagate_orderly_shutdown(actor_system& sys, cluster& sys_cluster,
                                const node_config& cfg,
                                const node_manifest& manifest,
                                const shutdown_request& trigger) {
  if (!cfg.shutdown_children_on_exit && !cfg.shutdown_parent_on_exit)
    return;
  sys.println("[{}] shutdown source={} initiator='{}' reason={}",
              manifest.node_name, to_string(trigger.source),
              trigger.initiator.empty() ? manifest.node_name : trigger.initiator,
              trigger.reason.empty() ? "<none>" : trigger.reason);
  auto subtree_node_names = propagate_shutdown_to_children(sys, sys_cluster,
                                                           cfg, manifest,
                                                           trigger);
  wait_for_subtree_shutdown(sys, sys_cluster, cfg, manifest, subtree_node_names);
}

bool start_managed_node(actor_system& sys, const node_config& cfg,
                        cluster& sys_cluster, const node_manifest& manifest,
                        const actor& monitor_actor,
                        std::initializer_list<actor> actors,
                        bool attach_parent) {
  if (!open_node_port(sys, cfg.bind, cfg.port)) {
    shutdown_actors(actors);
    return false;
  }
  if (!sys_cluster.register_with_master(manifest, monitor_actor)) {
    shutdown_actors(actors);
    return false;
  }
  if (attach_parent && !sys_cluster.attach_to_parent_region(manifest,
                                                            monitor_actor)) {
    sys_cluster.unregister_from_master(manifest.node_name);
    shutdown_actors(actors);
    return false;
  }
  return true;
}

void stop_managed_node(cluster& sys_cluster, const node_manifest& manifest,
                       std::initializer_list<actor> actors,
                       bool attach_parent) {
  if (attach_parent)
    sys_cluster.detach_from_parent_region(manifest);
  sys_cluster.unregister_from_master(manifest.node_name);
  shutdown_actors(actors);
}
