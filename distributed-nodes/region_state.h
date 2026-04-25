#pragma once

#include "app.hpp"


struct region_state {
  explicit region_state(event_based_actor* selfptr, node_manifest manifest)
    : self(selfptr), info(std::move(manifest)) {
    // nop
  }

  region_snapshot make_snapshot() const {
    region_snapshot snapshot;
    snapshot.region_name = info.node_name;
    for (const auto& [_, child] : children)
      snapshot.children.push_back(child);
    sort_manifests(snapshot.children);
    return snapshot;
  }

  behavior make_behavior() {
    return {
      [this](node_describe_atom) {
        return info;
      },
      [this](region_attach_atom, node_manifest child) {
        auto existed = children.find(child.node_name) != children.end();
        children[child.node_name] = child;
        self->println("[region:{}] {} child '{}' ({})", info.node_name,
                      existed ? "updated" : "attached", child.node_name,
                      to_string(child.kind));
        return register_reply{true, existed ? "child updated" : "child attached"};
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
      [this](region_status_atom) {
        return make_snapshot();
      },
    };
  }

  event_based_actor* self;
  node_manifest info;
  std::unordered_map<std::string, node_manifest> children;
};
