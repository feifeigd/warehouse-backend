#pragma once

#include "cluster.hpp"

#include <caf/policy/select_all.hpp>
#include <caf/typed_actor.hpp>

#include <queue>
#include <unordered_map>
#include <unordered_set>

using node_shutdown_target_actor
  = typed_actor<result<register_reply>(node_shutdown_atom, shutdown_request)>;

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

std::unordered_set<std::string> collect_subtree_node_names(
  const topology_snapshot& snapshot, const std::string& root_name) {
  std::unordered_set<std::string> result;
  std::unordered_map<std::string, std::vector<std::string>> children_by_parent;
  for (const auto& node : snapshot.nodes)
    children_by_parent[node.parent].push_back(node.node_name);

  std::queue<std::string> pending;
  pending.push(root_name);
  while (!pending.empty()) {
    auto parent = std::move(pending.front());
    pending.pop();
    auto iter = children_by_parent.find(parent);
    if (iter == children_by_parent.end())
      continue;
    for (const auto& child : iter->second) {
      result.insert(child);
      pending.push(child);
    }
  }
  return result;
}

bool topology_contains_any(const topology_snapshot& snapshot,
                           const std::unordered_set<std::string>& node_names) {
  return std::any_of(snapshot.nodes.begin(), snapshot.nodes.end(),
                     [&](const node_manifest& node) {
                       return node_names.contains(node.node_name);
                     });
}

std::vector<std::string> unique_child_names(
  const std::vector<std::string>& child_names) {
  std::vector<std::string> result;
  std::unordered_set<std::string> seen;
  result.reserve(child_names.size());
  for (const auto& child_name : child_names) {
    if (!seen.insert(child_name).second)
      continue;
    result.push_back(child_name);
  }
  return result;
}

std::unordered_set<std::string> subtree_names_for_fallback(
  const std::optional<topology_snapshot>& topology, const std::string& child_name) {
  auto result = std::unordered_set<std::string>{child_name};
  if (!topology)
    return result;
  auto subtree = collect_subtree_node_names(*topology, child_name);
  result.insert(subtree.begin(), subtree.end());
  return result;
}

std::vector<std::string> request_shutdown_on_children(
  scoped_actor& self, actor_system& sys, cluster& sys_cluster,
  const node_config& cfg, const shutdown_request& request,
  const std::vector<std::string>& child_names) {
  std::vector<node_shutdown_target_actor> controls;
  std::vector<std::string> target_child_names;
  std::vector<std::string> failed_child_names;
  controls.reserve(child_names.size());
  target_child_names.reserve(child_names.size());
  for (const auto& child_name : child_names) {
    auto control = sys_cluster.lookup_node_control_actor(self, child_name);
    if (!control) {
      failed_child_names.push_back(child_name);
      continue;
    }
    controls.emplace_back(actor_cast<node_shutdown_target_actor>(control));
    target_child_names.push_back(child_name);
  }
  if (!controls.empty()) {
    auto all_failed = false;
    self->fan_out_request<policy::select_all>(controls,
                                              sys_cluster.shutdown_request_timeout(),
                                              node_shutdown_atom_v, request)
      .receive(
        [&](std::vector<register_reply> replies) {
          for (size_t index = 0; index < replies.size(); ++index) {
            if (replies[index].ok)
              continue;
            auto child_name = target_child_names[index];
            failed_child_names.push_back(child_name);
            sys.println("[{}] child shutdown request for '{}' was rejected: {}",
                        cfg.name, child_name, replies[index].message);
          }
        },
        [&](const error& err) {
          all_failed = true;
          sys.println("[{}] child shutdown fan-out failed: {}", cfg.name,
                      to_string(err));
        }
      );
    if (all_failed) {
      failed_child_names.insert(failed_child_names.end(), target_child_names.begin(),
                                target_child_names.end());
    }
  }
  return unique_child_names(failed_child_names);
}

std::vector<std::string> collect_present_failed_children(
  scoped_actor& self, actor_system& sys, cluster& sys_cluster,
  const node_config& cfg,
  const std::unordered_map<std::string, std::unordered_set<std::string>>&
    subtree_names_by_child,
  const std::vector<std::string>& failed_child_names) {
  auto unique_failed_child_names = unique_child_names(failed_child_names);
  if (unique_failed_child_names.empty())
    return {};
  auto topology = sys_cluster.request_topology(self);
  if (!topology) {
    for (const auto& child_name : unique_failed_child_names) {
      sys.println("[{}] child shutdown fallback for '{}' could not load topology",
                  cfg.name, child_name);
    }
    return unique_failed_child_names;
  }
  std::vector<std::string> pending_child_names;
  pending_child_names.reserve(unique_failed_child_names.size());
  for (const auto& child_name : unique_failed_child_names) {
    auto iter = subtree_names_by_child.find(child_name);
    auto node_names = iter == subtree_names_by_child.end()
                        ? std::unordered_set<std::string>{child_name}
                        : iter->second;
    if (!topology_contains_any(*topology, node_names)) {
      sys.println("[{}] child subtree '{}' already stopped during fallback",
                  cfg.name, child_name);
      continue;
    }
    pending_child_names.push_back(child_name);
  }
  return pending_child_names;
}

void retry_failed_child_shutdowns(
  scoped_actor& self, actor_system& sys, cluster& sys_cluster,
  const node_config& cfg, const shutdown_request& request,
  const std::unordered_map<std::string, std::unordered_set<std::string>>&
    subtree_names_by_child,
  const std::vector<std::string>& failed_child_names) {
  constexpr size_t max_retry_attempts = 2;
  auto pending_child_names = collect_present_failed_children(
    self, sys, sys_cluster, cfg, subtree_names_by_child, failed_child_names);
  for (size_t attempt = 1; attempt <= max_retry_attempts
                           && !pending_child_names.empty();
       ++attempt) {
    sys.println("[{}] retrying child shutdown for [{}] ({}/{})", cfg.name,
                join_strings(pending_child_names), attempt,
                max_retry_attempts);
    pending_child_names = request_shutdown_on_children(
      self, sys, sys_cluster, cfg, request, pending_child_names);
    if (pending_child_names.empty())
      return;
    pending_child_names = collect_present_failed_children(
      self, sys, sys_cluster, cfg, subtree_names_by_child,
      pending_child_names);
    if (!pending_child_names.empty() && attempt < max_retry_attempts)
      std::this_thread::sleep_for(cfg.cluster_retry_interval());
  }
  for (const auto& child_name : pending_child_names) {
    sys.println("[{}] child shutdown retry exhausted for '{}'", cfg.name,
                child_name);
  }
}

void propagate_shutdown_to_children(actor_system& sys, cluster& sys_cluster,
                                    const node_config& cfg,
                                    const node_manifest& manifest,
                                    const shutdown_request& trigger) {
  if (!cfg.shutdown_children_on_exit)
    return;
  scoped_actor self{sys};
  auto children = sys_cluster.request_children(self, manifest.node_name);
  if (!children)
    return;
  auto topology = sys_cluster.request_topology(self);
  auto request = make_child_shutdown_request(manifest, trigger);
  auto skip_child = trigger.source == shutdown_source::child
                    ? trigger.source_node
                    : std::string{};
  std::unordered_map<std::string, std::unordered_set<std::string>>
    subtree_names_by_child;
  std::vector<std::string> initial_child_names;
  initial_child_names.reserve(children->children.size());
  for (const auto& child : children->children) {
    if (!skip_child.empty() && child.node_name == skip_child)
      continue;
    initial_child_names.push_back(child.node_name);
    subtree_names_by_child.emplace(child.node_name,
                                   subtree_names_for_fallback(topology,
                                                              child.node_name));
  }
  auto failed_child_names = request_shutdown_on_children(
    self, sys, sys_cluster, cfg, request, initial_child_names);
  retry_failed_child_shutdowns(self, sys, sys_cluster, cfg, request,
                               subtree_names_by_child, failed_child_names);
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
  propagate_shutdown_to_children(sys, sys_cluster, cfg, manifest, trigger);
}
