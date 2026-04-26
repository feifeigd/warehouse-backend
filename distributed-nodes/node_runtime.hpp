#pragma once

#include "node_types.hpp"
#include "cluster.hpp"
#include "node_heartbeat.hpp"
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
  if (manifest.parent == "master")
    return;
  if (trigger.source == shutdown_source::parent)
    return;
  scoped_actor self{sys};
  sys_cluster.request_node_shutdown(self, manifest.parent,
                                    make_parent_shutdown_request(manifest,
                                                                 trigger));
}

// Orderly shutdown: stop child subtrees before this node notifies its parent.
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

void run_managed_node_lifecycle(actor_system& sys, const node_config& cfg,
                                cluster& sys_cluster,
                                const node_manifest& manifest,
                                const std::shared_ptr<shutdown_signal>& shutdown,
                                const actor& monitor_actor,
                                std::initializer_list<actor> actors,
                                bool attach_parent, bool heartbeat_parent,
                                const std::string& role) {
  node_heartbeat heartbeats;

  do {
    if (!start_managed_node(sys, cfg, sys_cluster, manifest, monitor_actor,
                            actors, attach_parent)) {
      break;
    }
    if (!heartbeats.start(sys, sys_cluster, cfg, manifest, monitor_actor,
                          heartbeat_parent)) {
      stop_managed_node(sys_cluster, manifest, actors, attach_parent);
      break;
    }

    auto trigger = shutdown->wait(sys, role, manifest.node_name, cfg.lifetime);
    heartbeats.stop();
    propagate_orderly_shutdown(sys, sys_cluster, cfg, manifest, trigger);
    stop_managed_node(sys_cluster, manifest, actors, attach_parent);
    shutdown->complete_shutdown(register_reply{true, "shutdown complete"});
    propagate_shutdown_to_parent(sys, sys_cluster, cfg, manifest, trigger);
  } while (false);

  shutdown_actors(actors);
}

void run_master_node_lifecycle(actor_system& sys, const node_config& cfg,
                               cluster& sys_cluster,
                               const node_manifest& manifest,
                               const std::shared_ptr<shutdown_signal>& shutdown,
                               std::initializer_list<actor> actors) {
  if (!open_node_port(sys, cfg.bind, cfg.port)) {
    shutdown_actors(actors);
    return;
  }

  sys.println("[master] '{}' listening on {}:{}", cfg.name, cfg.host, cfg.port);
  auto trigger = shutdown->wait(sys, "master", manifest.node_name, cfg.lifetime);
  propagate_orderly_shutdown(sys, sys_cluster, cfg, manifest, trigger);
  shutdown->complete_shutdown(register_reply{true, "shutdown complete"});
  shutdown_actors(actors);
}
