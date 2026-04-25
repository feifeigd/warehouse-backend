#pragma once

#include "app.hpp"


struct region_state {
  struct child_slot {
    node_manifest manifest;
    actor monitor_actor;
    steady_clock_type::time_point expires_at = never_expires();
  };

  explicit region_state(event_based_actor* selfptr, node_manifest manifest,
                        std::chrono::seconds lease_ttl)
    : self(selfptr), info(std::move(manifest)), lease_ttl(lease_ttl) {
    schedule_maintenance();
  }

  region_snapshot make_snapshot() const {
    region_snapshot snapshot;
    snapshot.region_name = info.node_name;
    for (const auto& [_, child] : children)
      snapshot.children.push_back(child.manifest);
    sort_manifests(snapshot.children);
    return snapshot;
  }

  void schedule_maintenance() {
    self->delayed_send(self, k_maintenance_step, maintenance_tick_atom_v);
  }

  steady_clock_type::time_point child_expiry() const {
    if (lease_ttl.count() == 0)
      return never_expires();
    return lease_deadline(lease_ttl);
  }

  void upsert(node_manifest child, actor monitor_actor) {
    auto child_name = child.node_name;
    auto& slot = children[child_name];
    auto same_monitor = slot.monitor_actor && monitor_actor
                        && slot.monitor_actor.address() == monitor_actor.address();
    if (slot.monitor_actor && !same_monitor)
      self->demonitor(slot.monitor_actor);
    slot.manifest = std::move(child);
    slot.monitor_actor = monitor_actor;
    slot.expires_at = child_expiry();
    if (slot.monitor_actor && !same_monitor)
      self->monitor(slot.monitor_actor);
  }

  void erase_child(std::unordered_map<std::string, child_slot>::iterator iter,
                   bool demonitor_actor) {
    if (demonitor_actor && iter->second.monitor_actor)
      self->demonitor(iter->second.monitor_actor);
    children.erase(iter);
  }

  bool erase_child_by_monitor(const actor_addr& source, const error& reason) {
    for (auto iter = children.begin(); iter != children.end(); ++iter) {
      if (!iter->second.monitor_actor)
        continue;
      if (iter->second.monitor_actor.address() != source)
        continue;
      self->println("[region:{}] child '{}' ({}) went down: {}", info.node_name,
                    iter->second.manifest.node_name,
                    to_string(iter->second.manifest.kind), to_string(reason));
      erase_child(iter, false);
      return true;
    }
    return false;
  }

  bool touch(const std::string& child_name) {
    auto iter = children.find(child_name);
    if (iter == children.end())
      return false;
    iter->second.expires_at = child_expiry();
    return true;
  }

  void prune_expired() {
    if (lease_ttl.count() == 0)
      return;
    auto now = steady_clock_type::now();
    std::vector<std::string> expired;
    for (const auto& [child_name, slot] : children) {
      if (slot.expires_at <= now)
        expired.push_back(child_name);
    }
    for (const auto& child_name : expired) {
      auto iter = children.find(child_name);
      if (iter == children.end())
        continue;
      self->println("[region:{}] expired child '{}' ({})", info.node_name,
                    child_name, to_string(iter->second.manifest.kind));
      erase_child(iter, true);
    }
  }

  behavior make_behavior() {
    return {
      [this](node_describe_atom) {
        return info;
      },
      [this](region_attach_atom, node_registration registration) {
        prune_expired();
        auto child = std::move(registration.manifest);
        auto existed = children.find(child.node_name) != children.end();
        auto child_name = child.node_name;
        auto kind = child.kind;
        upsert(std::move(child), registration.monitor_actor);
        self->println("[region:{}] {} child '{}' ({})", info.node_name,
                      existed ? "updated" : "attached", child_name,
                      to_string(kind));
        return register_reply{true, existed ? "child updated" : "child attached"};
      },
      [this](region_heartbeat_atom, const std::string& child_name) {
        prune_expired();
        if (touch(child_name))
          return register_reply{true, "child refreshed"};
        return register_reply{false, "child not found"};
      },
      [this](region_detach_atom, const std::string& child_name) {
        if (auto iter = children.find(child_name); iter != children.end()) {
          erase_child(iter, true);
          self->println("[region:{}] detached child '{}'", info.node_name,
                        child_name);
          return register_reply{true, "child detached"};
        }
        return register_reply{false, "child not found"};
      },
      [this](const down_msg& msg) {
        erase_child_by_monitor(msg.source, msg.reason);
      },
      [this](maintenance_tick_atom) {
        prune_expired();
        schedule_maintenance();
      },
      [this](region_status_atom) {
        prune_expired();
        return make_snapshot();
      },
    };
  }

  event_based_actor* self;
  node_manifest info;
  std::chrono::seconds lease_ttl;
  std::unordered_map<std::string, child_slot> children;
};
