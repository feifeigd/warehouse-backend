#pragma once

#include "membership_registry.h"


class master_state {
public:
  explicit master_state(event_based_actor* selfptr, node_manifest manifest,
                        std::chrono::seconds lease_ttl)
    : self(selfptr), lease_ttl(lease_ttl), nodes(selfptr) {
    upsert(std::move(manifest), {});
    schedule_maintenance();
  }

  topology_snapshot make_topology() const {
    topology_snapshot snapshot;
    snapshot.nodes.reserve(nodes.size());
    nodes.for_each_manifest([&](const node_manifest& manifest) {
      snapshot.nodes.push_back(manifest);
    });
    sort_manifests(snapshot.nodes);
    return snapshot;
  }

  child_snapshot make_children(const std::string& parent_name) const {
    child_snapshot snapshot;
    snapshot.parent = parent_name;
    nodes.for_each_manifest([&](const node_manifest& manifest) {
      if (manifest.parent == parent_name)
        snapshot.children.push_back(manifest);
    });
    sort_manifests(snapshot.children);
    return snapshot;
  }

  // Re-arm the periodic maintenance tick that prunes expired nodes.
  void schedule_maintenance() {
    self->delayed_send(self, k_maintenance_step, maintenance_tick_atom_v);
  }

  steady_clock_type::time_point expires_at(node_kind kind) const {
    if (kind == node_kind::master || lease_ttl.count() == 0)
      return never_expires();
    return lease_deadline(lease_ttl);
  }

  void upsert(node_manifest manifest, actor monitor_actor) {
    nodes.upsert(std::move(manifest), monitor_actor,
                 [this](const node_manifest& item) {
                   return expires_at(item.kind);
                 });
  }

  bool touch(const std::string& node_name) {
    return nodes.touch(node_name, [this](const node_manifest& item) {
      return expires_at(item.kind);
    });
  }

  void prune_expired() {
    if (lease_ttl.count() == 0)
      return;
    nodes.prune_expired(steady_clock_type::now(),
                        [](const node_manifest& manifest) {
                          return manifest.kind == node_kind::master;
                        },
                        [this](const node_manifest& manifest) {
                          self->println("[master] expired node '{}' ({})",
                                        manifest.node_name,
                                        to_string(manifest.kind));
                        });
  }

  behavior make_behavior() {
    return {
      [this](master_register_atom, node_registration registration) {
        prune_expired();
        auto manifest = std::move(registration.manifest);
        auto existed = nodes.contains(manifest.node_name);
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
        if (nodes.erase(node_name, true)) {
          self->println("[master] unregistered node '{}'", node_name);
          return register_reply{true, "node removed"};
        }
        return register_reply{false, "node not found"};
      },
      [this](const down_msg& msg) {
        nodes.erase_by_monitor(msg.source, msg.reason,
                               [this](const node_manifest& manifest,
                                      const error& reason) {
                                 self->println("[master] node '{}' ({}) went down: {}",
                                               manifest.node_name,
                                               to_string(manifest.kind),
                                               to_string(reason));
                               });
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
        auto manifest = nodes.find_manifest(node_name);
        if (!manifest)
          return make_error(sec::no_such_key);
        auto found = std::find(manifest->exported_actors.begin(),
                               manifest->exported_actors.end(), actor_name);
        if (found == manifest->exported_actors.end())
          return make_error(sec::no_such_key);
        return actor_route{
          manifest->node_name,
          manifest->kind,
          manifest->host,
          manifest->port,
          actor_name,
          manifest->parent,
        };
      },
    };
  }

  event_based_actor* self;
  std::chrono::seconds lease_ttl;
  monitored_node_registry nodes;
};
