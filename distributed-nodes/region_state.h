#pragma once

#include "app.hpp"


struct region_state {
  struct child_slot {
    node_manifest manifest;
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

  void upsert(node_manifest child) {
    auto child_name = child.node_name;
    children[std::move(child_name)] = child_slot{std::move(child), child_expiry()};
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
      children.erase(iter);
    }
  }

  behavior make_behavior() {
    return {
      [this](node_describe_atom) {
        return info;
      },
      [this](region_attach_atom, node_manifest child) {
        prune_expired();
        auto existed = children.find(child.node_name) != children.end();
        auto child_name = child.node_name;
        auto kind = child.kind;
        upsert(std::move(child));
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
        auto erased = children.erase(child_name);
        if (erased > 0) {
          self->println("[region:{}] detached child '{}'", info.node_name,
                        child_name);
          return register_reply{true, "child detached"};
        }
        return register_reply{false, "child not found"};
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
