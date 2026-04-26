#pragma once

#include "cluster.hpp"

struct node_heartbeat_state {
  event_based_actor* self;
  actor master_actor;
  actor parent_actor;
  std::string master_host;
  uint16_t master_port = 0;
  node_manifest manifest;
  actor monitor_actor;
  bool heartbeat_parent = false;
  std::chrono::seconds interval;

  node_heartbeat_state(event_based_actor* selfptr, actor initial_master,
                       const node_config& config, node_manifest node,
                       actor monitor, bool check_parent)
    : self(selfptr),
      master_actor(std::move(initial_master)),
      master_host(config.master_host),
      master_port(config.master_port),
      manifest(std::move(node)),
      monitor_actor(std::move(monitor)),
      heartbeat_parent(check_parent),
      interval(std::chrono::seconds{config.node_heartbeat_seconds}) {
    // nop
  }

  void schedule_next() {
    self->delayed_send(self, interval, heartbeat_tick_atom_v);
  }

  void reconnect_master() {
    master_actor = {};
    master_actor = lookup_remote_actor(self->system(), master_host, master_port,
                                       k_master_control, 0ms, 0ms);
    if (!master_actor) {
      self->println("[{}] master reconnect failed", manifest.node_name);
      schedule_next();
      return;
    }
    register_with_master();
  }

  void register_with_master() {
    if (!master_actor) {
      reconnect_master();
      return;
    }
    self->request(master_actor, 10s, master_register_atom_v,
                  node_registration{manifest, monitor_actor}).then(
      [this](const register_reply& reply) {
        if (reply.ok) {
          self->println("[{}] re-registered with master: {}",
                        manifest.node_name, reply.message);
          heartbeat_parent_if_needed();
        } else {
          self->println("[{}] master re-registration rejected: {}",
                        manifest.node_name, reply.message);
          schedule_next();
        }
      },
      [this](const error& err) {
        self->println("[{}] master re-registration failed: {}",
                      manifest.node_name, to_string(err));
        master_actor = {};
        schedule_next();
      }
    );
  }

  void resolve_parent_region() {
    if (!heartbeat_parent || manifest.parent.empty()) {
      schedule_next();
      return;
    }
    if (!master_actor) {
      reconnect_master();
      return;
    }
    self->request(master_actor, 10s, master_resolve_atom_v, manifest.parent,
                  k_region_router).then(
      [this](const actor_route& route) {
        lookup_parent_region(route);
      },
      [this](const error& err) {
        self->println("[{}] parent resolve failed: {}", manifest.node_name,
                      to_string(err));
        schedule_next();
      }
    );
  }

  void lookup_parent_region(const actor_route& route) {
    parent_actor = lookup_remote_actor(self->system(), route.host, route.port,
                                       route.actor_name, 0ms, 0ms);
    if (!parent_actor) {
      self->println("[{}] parent reconnect failed", manifest.node_name);
      schedule_next();
      return;
    }
    attach_to_parent_region();
  }

  void attach_to_parent_region() {
    if (!parent_actor) {
      resolve_parent_region();
      return;
    }
    self->request(parent_actor, 10s, region_attach_atom_v,
                  node_registration{manifest, monitor_actor}).then(
      [this](const register_reply& reply) {
        if (reply.ok) {
          self->println("[{}] parent attach: {}", manifest.node_name,
                        reply.message);
        } else {
          self->println("[{}] parent attach rejected: {}", manifest.node_name,
                        reply.message);
          parent_actor = {};
        }
        schedule_next();
      },
      [this](const error& err) {
        self->println("[{}] parent attach failed: {}", manifest.node_name,
                      to_string(err));
        parent_actor = {};
        schedule_next();
      }
    );
  }

  void heartbeat_parent_if_needed() {
    if (!heartbeat_parent) {
      schedule_next();
      return;
    }
    if (!parent_actor) {
      resolve_parent_region();
      return;
    }
    self->request(parent_actor, 10s, region_heartbeat_atom_v,
                  manifest.node_name).then(
      [this](const register_reply& reply) {
        if (!reply.ok) {
          self->println("[{}] parent heartbeat rejected: {}",
                        manifest.node_name, reply.message);
          parent_actor = {};
          resolve_parent_region();
          return;
        }
        schedule_next();
      },
      [this](const error& err) {
        self->println("[{}] parent heartbeat failed: {}", manifest.node_name,
                      to_string(err));
        parent_actor = {};
        resolve_parent_region();
      }
    );
  }

  behavior make_behavior() {
    schedule_next();
    return {
      [this](heartbeat_tick_atom) {
        if (!master_actor) {
          reconnect_master();
          return;
        }
        self->request(master_actor, 10s, master_heartbeat_atom_v,
                      manifest.node_name).then(
          [this](const register_reply& reply) {
            if (!reply.ok) {
              self->println("[{}] master heartbeat rejected: {}",
                            manifest.node_name, reply.message);
              register_with_master();
              return;
            }
            heartbeat_parent_if_needed();
          },
          [this](const error& err) {
            self->println("[{}] master heartbeat failed: {}",
                          manifest.node_name, to_string(err));
            reconnect_master();
          }
        );
      },
    };
  }
};

/// Sends periodic heartbeats to the parent region and master.
class node_heartbeat {
public:
  node_heartbeat() = default;

  ~node_heartbeat() {
    stop();
  }

  bool start(actor_system& sys, cluster& sys_cluster, const node_config& cfg,
             const node_manifest& manifest, const actor& monitor_actor,
             bool heartbeat_parent) {
    stop();
    if (cfg.node_heartbeat_seconds == 0)
      return true; // Heartbeats disabled by config.
    worker_ = sys.spawn(actor_from_state<node_heartbeat_state>,
                        sys_cluster.master_actor(), cfg, manifest,
                        monitor_actor, heartbeat_parent);
    return static_cast<bool>(worker_);
  }

  void stop() {
    if (worker_) {
      anon_send_exit(worker_, exit_reason::user_shutdown);
      worker_ = {};
    }
  }

private:
  actor worker_;
};
