#pragma once

#include "app.hpp"


struct master_state {
  explicit master_state(event_based_actor* selfptr, node_manifest manifest)
    : self(selfptr) {
    nodes.emplace(manifest.node_name, std::move(manifest));
  }

  topology_snapshot make_topology() const {
    topology_snapshot snapshot;
    snapshot.nodes.reserve(nodes.size());
    for (const auto& [_, node] : nodes)
      snapshot.nodes.push_back(node);
    sort_manifests(snapshot.nodes);
    return snapshot;
  }

  child_snapshot make_children(const std::string& parent_name) const {
    child_snapshot snapshot;
    snapshot.parent = parent_name;
    for (const auto& [_, node] : nodes) {
      if (node.parent == parent_name)
        snapshot.children.push_back(node);
    }
    sort_manifests(snapshot.children);
    return snapshot;
  }

  behavior make_behavior() {
    return {
      [this](master_register_atom, node_manifest manifest) {
        auto existed = nodes.find(manifest.node_name) != nodes.end();
        nodes[manifest.node_name] = manifest;
        self->println("[master] {} node '{}' ({}) parent={} actors=[{}]",
                      existed ? "updated" : "registered", manifest.node_name,
                      to_string(manifest.kind),
                      manifest.parent.empty() ? "<root>" : manifest.parent,
                      join_strings(manifest.exported_actors));
        return register_reply{true, existed ? "node updated" : "node registered"};
      },
      [this](master_unregister_atom, const std::string& node_name) {
        auto erased = nodes.erase(node_name);
        if (erased > 0) {
          self->println("[master] unregistered node '{}'", node_name);
          return register_reply{true, "node removed"};
        }
        return register_reply{false, "node not found"};
      },
      [this](master_topology_atom) {
        return make_topology();
      },
      [this](master_children_atom, const std::string& parent_name) {
        return make_children(parent_name);
      },
      [this](master_resolve_atom, const std::string& node_name,
             const std::string& actor_name) -> result<actor_route> {
        auto iter = nodes.find(node_name);
        if (iter == nodes.end())
          return make_error(sec::no_such_key);
        const auto& manifest = iter->second;
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
  std::unordered_map<std::string, node_manifest> nodes;
};

