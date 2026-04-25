#pragma once

#include "membership_registry.h"


struct region_state {
  explicit region_state(event_based_actor* selfptr, node_manifest manifest,
                        std::chrono::seconds lease_ttl)
    : self(selfptr), info(std::move(manifest)), lease_ttl(lease_ttl),
      children(selfptr) {
    schedule_maintenance();
  }

  region_snapshot make_snapshot() const {
    region_snapshot snapshot;
    snapshot.region_name = info.node_name;
    snapshot.children.reserve(children.size());
    children.for_each_manifest([&](const node_manifest& child) {
      snapshot.children.push_back(child);
    });
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
    children.upsert(std::move(child), monitor_actor,
                    [this](const node_manifest&) {
                      return child_expiry();
                    });
  }

  bool touch(const std::string& child_name) {
    return children.touch(child_name, [this](const node_manifest&) {
      return child_expiry();
    });
  }

  void prune_expired() {
    if (lease_ttl.count() == 0)
      return;
    children.prune_expired(steady_clock_type::now(),
                           [](const node_manifest&) {
                             return false;
                           },
                           [this](const node_manifest& child) {
                             self->println("[region:{}] expired child '{}' ({})",
                                           info.node_name, child.node_name,
                                           to_string(child.kind));
                           });
  }

  behavior make_behavior() {
    return {
      [this](node_describe_atom) {
        return info;
      },
      [this](region_attach_atom, node_registration registration) {
        prune_expired();
        auto child = std::move(registration.manifest);
        auto existed = children.contains(child.node_name);
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
        if (children.erase(child_name, true)) {
          self->println("[region:{}] detached child '{}'", info.node_name,
                        child_name);
          return register_reply{true, "child detached"};
        }
        return register_reply{false, "child not found"};
      },
      [this](const down_msg& msg) {
        children.erase_by_monitor(msg.source, msg.reason,
                                  [this](const node_manifest& child,
                                         const error& reason) {
                                    self->println("[region:{}] child '{}' ({}) went down: {}",
                                                  info.node_name,
                                                  child.node_name,
                                                  to_string(child.kind),
                                                  to_string(reason));
                                  });
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
  monitored_node_registry children;
};
