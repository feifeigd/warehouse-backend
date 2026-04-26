#pragma once

#include "node_types.hpp"
#include "cluster.hpp"
#include "node_heartbeat.hpp"
#include "node_shutdown_flow.hpp"
#include "shutdown_signal.hpp"

behavior node_control_actor_fun(event_based_actor* self, node_manifest manifest,
                                std::shared_ptr<shutdown_signal> signal) {
  return {
    [manifest](node_describe_atom) {
      return manifest;  // 
    },
    [self, manifest, signal](node_shutdown_atom,
                             shutdown_request request) -> result<register_reply> {
      const auto source = request.source;
      auto source_node = request.source_node.empty() ? "<unknown>"
                                                     : request.source_node;
      auto reason = request.reason;
      auto shutdown_actor = signal->actor_handle();
      if (!shutdown_actor)
        return register_reply{false, "shutdown signal not started"};
      auto response = self->make_response_promise();
      self->request(shutdown_actor, 5s, shutdown_signal_request_atom_v,
                    std::move(request)).then(
        [self, manifest, signal, source, source_node, reason,
         response](const register_reply& reply) mutable {
          auto accepted = reply.message == "shutdown requested";
          self->println("[{}:{}] shutdown {} from '{}' ({}) reason={}",
                        to_string(manifest.kind), manifest.node_name,
                        accepted ? "requested" : "already pending",
                        source_node, to_string(source), reason);
          if (source != shutdown_source::parent) {
            response.deliver(reply);
            return;
          }
          signal->add_completion_waiter(response);
        },
        [response](const error& err) mutable {
          response.deliver(register_reply{
            false,
            "shutdown request failed: " + to_string(err),
          });
        }
      );
      if (source == shutdown_source::parent) {
        return response;
      }
      return response;
    },
  };
}


bool open_node_port(actor_system& sys, const std::string& bind_addr,
                    uint16_t port) {
  const char* listen_addr = bind_addr.empty() ? nullptr : bind_addr.c_str();
  auto actual = sys.middleman().open(port, listen_addr, true);
  if (!actual) {
    sys.println("failed to open port {}: {}", port, to_string(actual.error()));
    return false;
  }
  return true;
}

void print_tree(actor_system& sys, const topology_snapshot& snapshot,
                const std::string& parent, int depth = 0) {
  std::vector<node_manifest> children;
  for (const auto& node : snapshot.nodes) {
    if (node.parent == parent)
      children.push_back(node);
  }
  sort_manifests(children);
  for (const auto& node : children) {
    auto indent = std::string(static_cast<size_t>(depth) * 2, ' ');
    sys.println("{}- {} [{}] {}:{} actors=[{}]", indent, node.node_name,
                to_string(node.kind), node.host, node.port,
                join_strings(node.exported_actors));
    print_tree(sys, snapshot, node.node_name, depth + 1);
  }
}

bool start_managed_node(actor_system& sys, const node_config& cfg,
                        cluster& sys_cluster, const node_manifest& manifest,
                        const actor& monitor_actor,
                        std::initializer_list<actor> actors,
                        bool attach_parent) {
  if (!open_node_port(sys, cfg.bind, cfg.port)) {
    shutdown_actors(actors);
    return false;
  }
  if (!sys_cluster.register_with_master(manifest, monitor_actor)) {
    shutdown_actors(actors);
    return false;
  }
  if (attach_parent && !sys_cluster.attach_to_parent_region(manifest,
                                                            monitor_actor)) {
    sys_cluster.unregister_from_master(manifest.node_name);
    shutdown_actors(actors);
    return false;
  }
  return true;
}

void stop_managed_node(cluster& sys_cluster, const node_manifest& manifest,
                       std::initializer_list<actor> actors,
                       bool attach_parent) {
  if (attach_parent)
    sys_cluster.detach_from_parent_region(manifest);
  sys_cluster.unregister_from_master(manifest.node_name);
  shutdown_actors(actors);
}

void detach_managed_node(cluster& sys_cluster, const node_manifest& manifest,
                         bool attach_parent) {
  if (attach_parent)
    sys_cluster.detach_from_parent_region(manifest);
  sys_cluster.unregister_from_master(manifest.node_name);
}

void run_managed_node_lifecycle(actor_system& sys, const node_config& cfg,
                                cluster& sys_cluster,
                                const node_manifest& manifest,
                                const std::shared_ptr<shutdown_signal>& shutdown,
                                const actor& monitor_actor,
                                std::initializer_list<actor> actors,
                                bool attach_parent, bool heartbeat_parent,
                                const std::string& role) {
  node_heartbeat heartbeats;

  do {
    if (!start_managed_node(sys, cfg, sys_cluster, manifest, monitor_actor,
                            actors, attach_parent)) {
      break;
    }
    if (!heartbeats.start(sys, sys_cluster, cfg, manifest, monitor_actor,
                          heartbeat_parent)) {
      stop_managed_node(sys_cluster, manifest, actors, attach_parent);
      break;
    }

    auto trigger = shutdown->wait(sys, role, manifest.node_name, cfg.lifetime);
    heartbeats.stop();
    propagate_orderly_shutdown(sys, sys_cluster, cfg, manifest, trigger);
    detach_managed_node(sys_cluster, manifest, attach_parent);
    shutdown->complete_shutdown(register_reply{true, "shutdown complete"});
    shutdown_actors(actors);
    propagate_shutdown_to_parent(sys, sys_cluster, cfg, manifest, trigger);
  } while (false);

  shutdown_actors(actors);
}

void run_master_node_lifecycle(actor_system& sys, const node_config& cfg,
                               cluster& sys_cluster,
                               const node_manifest& manifest,
                               const std::shared_ptr<shutdown_signal>& shutdown,
                               std::initializer_list<actor> actors) {
  if (!open_node_port(sys, cfg.bind, cfg.port)) {
    shutdown_actors(actors);
    return;
  }

  sys.println("[master] '{}' listening on {}:{}", cfg.name, cfg.host, cfg.port);
  auto trigger = shutdown->wait(sys, "master", manifest.node_name, cfg.lifetime);
  propagate_orderly_shutdown(sys, sys_cluster, cfg, manifest, trigger);
  shutdown->complete_shutdown(register_reply{true, "shutdown complete"});
  shutdown_actors(actors);
}
