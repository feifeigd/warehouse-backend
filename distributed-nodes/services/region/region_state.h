#pragma once

#include "child_membership.h"


struct region_state {
  explicit region_state(event_based_actor* selfptr, node_manifest manifest,
                        std::chrono::seconds lease_ttl)
    : membership(selfptr, std::move(manifest), lease_ttl, "region") {
    membership.schedule_maintenance();
  }

  region_snapshot make_snapshot() const {
    region_snapshot snapshot;
    snapshot.region_name = membership.owner_manifest().node_name;
    snapshot.children = membership.snapshot_children();
    return snapshot;
  }

  behavior make_behavior() {
    return {
      [this](node_describe_atom) {
        return membership.owner_manifest();
      },
      [this](region_attach_atom, node_registration registration) {
        return membership.attach(std::move(registration));
      },
      [this](region_heartbeat_atom, const std::string& child_name) {
        return membership.heartbeat(child_name);
      },
      [this](region_detach_atom, const std::string& child_name) {
        return membership.detach(child_name);
      },
      [this](const down_msg& msg) {
        membership.handle_down(msg);
      },
      [this](maintenance_tick_atom) {
        membership.on_maintenance_tick();
      },
      [this](region_status_atom) {
        membership.prune_expired();
        return make_snapshot();
      },
    };
  }

  child_membership membership;
};
