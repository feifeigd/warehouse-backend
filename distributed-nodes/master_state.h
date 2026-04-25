#pragma once

#include "app.hpp"


class master_state {
  struct node_slot {
    node_manifest manifest;
    actor monitor_actor;
    steady_clock_type::time_point expires_at = never_expires();
  };

public:
  explicit master_state(event_based_actor* selfptr, node_manifest manifest,
                        std::chrono::seconds lease_ttl)
    : self(selfptr), lease_ttl(lease_ttl) {
    upsert(std::move(manifest), {});
    schedule_maintenance();
  }

  topology_snapshot make_topology() const {
    topology_snapshot snapshot;
    snapshot.nodes.reserve(nodes.size());
    for (const auto& [_, node] : nodes)
      snapshot.nodes.push_back(node.manifest);
    sort_manifests(snapshot.nodes);
    return snapshot;
  }

  child_snapshot make_children(const std::string& parent_name) const {
    child_snapshot snapshot;
    snapshot.parent = parent_name;
    for (const auto& [_, node] : nodes) {
      if (node.manifest.parent == parent_name)
        snapshot.children.push_back(node.manifest);
    }
    sort_manifests(snapshot.children);
    return snapshot;
  }

// private:
  // 相当于启动1秒定时器，每秒检查一次过期的节点
  void schedule_maintenance() {
    self->delayed_send(self, k_maintenance_step, maintenance_tick_atom_v);
  }

  steady_clock_type::time_point expires_at(node_kind kind) const {
    if (kind == node_kind::master || lease_ttl.count() == 0)
      return never_expires();
    return lease_deadline(lease_ttl);
  }

  void upsert(node_manifest manifest, actor monitor_actor) {
    auto expiry = expires_at(manifest.kind);
    auto node_name = manifest.node_name;
    auto& slot = nodes[node_name];
    auto same_monitor = slot.monitor_actor && monitor_actor
                        && slot.monitor_actor.address() == monitor_actor.address();
    if (slot.monitor_actor && !same_monitor)
      self->demonitor(slot.monitor_actor);
    slot.manifest = std::move(manifest);
    slot.monitor_actor = monitor_actor;
    slot.expires_at = expiry;
    if (slot.monitor_actor && !same_monitor)
      self->monitor(slot.monitor_actor);
  }

  void erase_node(std::unordered_map<std::string, node_slot>::iterator iter,
                  bool demonitor_actor) {
    if (demonitor_actor && iter->second.monitor_actor)
      self->demonitor(iter->second.monitor_actor);
    nodes.erase(iter);
  }

  bool erase_node_by_monitor(const actor_addr& source, const error& reason) {
    for (auto iter = nodes.begin(); iter != nodes.end(); ++iter) {
      if (!iter->second.monitor_actor)
        continue;
      if (iter->second.monitor_actor.address() != source)
        continue;
      self->println("[master] node '{}' ({}) went down: {}",
                    iter->second.manifest.node_name,
                    to_string(iter->second.manifest.kind), to_string(reason));
      erase_node(iter, false);
      return true;
    }
    return false;
  }

  bool touch(const std::string& node_name) {
    auto iter = nodes.find(node_name);
    if (iter == nodes.end())
      return false;
    // 心跳 刷新过期时间
    iter->second.expires_at = expires_at(iter->second.manifest.kind);
    return true;
  }

  void prune_expired() {
    if (lease_ttl.count() == 0)
      return;
    auto now = steady_clock_type::now();
    std::vector<std::string> expired;
    for (const auto& [node_name, slot] : nodes) {
      if (slot.manifest.kind == node_kind::master)
        continue;
      if (slot.expires_at <= now)
        expired.push_back(node_name);
    }
    for (const auto& node_name : expired) {
      auto iter = nodes.find(node_name);
      if (iter == nodes.end())
        continue;
      self->println("[master] expired node '{}' ({})", node_name,
                    to_string(iter->second.manifest.kind));
      erase_node(iter, true);
    }
  }

  behavior make_behavior() {
    return {
      [this](master_register_atom, node_registration registration) {
        prune_expired();
        auto manifest = std::move(registration.manifest);
        auto existed = nodes.find(manifest.node_name) != nodes.end();
        auto node_name = manifest.node_name;
        auto kind = manifest.kind;
        auto parent = manifest.parent.empty() ? "<root>" : manifest.parent;
        auto actors = manifest.exported_actors;
        upsert(std::move(manifest), registration.monitor_actor);
        self->println("[master] {} node '{}' ({}) parent={} actors=[{}]",
                      existed ? "updated" : "registered", node_name,
                      to_string(kind), parent, join_strings(actors));
        return register_reply{true, existed ? "node updated" : "node registered"};
      },
      [this](master_heartbeat_atom, const std::string& node_name) {
        prune_expired();
        if (touch(node_name))
          return register_reply{true, "node refreshed"};
        return register_reply{false, "node not found"};
      },
      [this](master_unregister_atom, const std::string& node_name) {
        if (auto iter = nodes.find(node_name); iter != nodes.end()) {
          erase_node(iter, true);
          self->println("[master] unregistered node '{}'", node_name);
          return register_reply{true, "node removed"};
        }
        return register_reply{false, "node not found"};
      },
      [this](const down_msg& msg) {
        erase_node_by_monitor(msg.source, msg.reason);
      },
      [this](maintenance_tick_atom) {
        prune_expired();
        schedule_maintenance();
      },
      [this](master_topology_atom) {
        prune_expired();
        return make_topology();
      },
      [this](master_children_atom, const std::string& parent_name) {
        prune_expired();
        return make_children(parent_name);
      },
      [this](master_resolve_atom, const std::string& node_name,
             const std::string& actor_name) -> result<actor_route> {
        prune_expired();
        auto iter = nodes.find(node_name);
        if (iter == nodes.end())
          return make_error(sec::no_such_key);
        const auto& manifest = iter->second.manifest;
        auto found = std::find(manifest.exported_actors.begin(),
                               manifest.exported_actors.end(), actor_name);
        if (found == manifest.exported_actors.end())
          return make_error(sec::no_such_key);
        return actor_route{
          manifest.node_name,
          manifest.kind,
          manifest.host,
          manifest.port,
          actor_name,
          manifest.parent,
        };
      },
    };
  }

  event_based_actor* self;
  std::chrono::seconds lease_ttl;
  std::unordered_map<std::string, node_slot> nodes;
};

