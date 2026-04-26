#pragma once

#include "cluster.hpp"

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
