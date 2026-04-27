#pragma once

#include "membership_registry.h"

class child_membership {
public:
  child_membership(event_based_actor* self, node_manifest owner,
                   std::chrono::seconds lease_ttl, std::string role_label)
    : self_(self), owner_(std::move(owner)), lease_ttl_(lease_ttl),
      role_label_(std::move(role_label)), children_(self) {
    // nop
  }

  const node_manifest& owner_manifest() const {
    return owner_;
  }

  std::vector<node_manifest> snapshot_children() const {
    std::vector<node_manifest> result;
    result.reserve(children_.size());
    children_.for_each_manifest([&](const node_manifest& child) {
      result.push_back(child);
    });
    sort_manifests(result);
    return result;
  }

  void schedule_maintenance() {
    self_->delayed_send(self_, k_maintenance_step, maintenance_tick_atom_v);
  }

  void on_maintenance_tick() {
    prune_expired();
    schedule_maintenance();
  }

  register_reply attach(node_registration registration) {
    prune_expired();
    auto child = std::move(registration.manifest);
    auto existed = children_.contains(child.node_name);
    auto child_name = child.node_name;
    auto kind = child.kind;
    children_.upsert(std::move(child), registration.monitor_actor,
                     [this](const node_manifest&) {
                       return child_expiry();
                     });
    self_->println("[{}:{}] {} child '{}' ({})", role_label_, owner_.node_name,
                   existed ? "updated" : "attached", child_name,
                   to_string(kind));
    return register_reply{true, existed ? "child updated" : "child attached"};
  }

  register_reply heartbeat(const std::string& child_name) {
    prune_expired();
    if (children_.touch(child_name, [this](const node_manifest&) {
          return child_expiry();
        })) {
      return register_reply{true, "child refreshed"};
    }
    return register_reply{false, "child not found"};
  }

  register_reply detach(const std::string& child_name) {
    if (children_.erase(child_name, true)) {
      self_->println("[{}:{}] detached child '{}'", role_label_, owner_.node_name,
                     child_name);
      return register_reply{true, "child detached"};
    }
    return register_reply{false, "child not found"};
  }

  void handle_down(const down_msg& msg) {
    children_.erase_by_monitor(msg.source, msg.reason,
                               [this](const node_manifest& child,
                                      const error& reason) {
                                 self_->println("[{}:{}] child '{}' ({}) went down: {}",
                                                role_label_, owner_.node_name,
                                                child.node_name,
                                                to_string(child.kind),
                                                to_string(reason));
                               });
  }

  void prune_expired() {
    if (lease_ttl_.count() == 0)
      return;
    children_.prune_expired(steady_clock_type::now(),
                            [](const node_manifest&) {
                              return false;
                            },
                            [this](const node_manifest& child) {
                              self_->println("[{}:{}] expired child '{}' ({})",
                                             role_label_, owner_.node_name,
                                             child.node_name,
                                             to_string(child.kind));
                            });
  }

private:
  steady_clock_type::time_point child_expiry() const {
    if (lease_ttl_.count() == 0)
      return never_expires();
    return lease_deadline(lease_ttl_);
  }

  event_based_actor* self_;
  node_manifest owner_;
  std::chrono::seconds lease_ttl_;
  std::string role_label_;
  monitored_node_registry children_;
};