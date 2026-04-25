#include "app.hpp"

#include <caf/caf_main.hpp>

struct client_config : node_config {
  client_config() : node_config("client", 0, {}, "region-a") {
    // nop
  }
};

namespace {

void print_topology(actor_system& sys, const topology_snapshot& topology) {
  sys.println("[client] topology");
  for (const auto& node : topology.nodes) {
    if (node.kind != node_kind::master)
      continue;
    sys.println("- {} [{}] {}:{}", node.node_name, to_string(node.kind),
                node.host, node.port);
    print_tree(sys, topology, node.node_name, 1);
  }
}

actor resolve_remote_actor(cluster& sys_cluster, scoped_actor& self,
                           actor_system& sys, const std::string& node_name,
                           const std::string& actor_name,
                           const char* actor_label) {
  auto route = sys_cluster.request_route(self, node_name, actor_name);
  if (!route)
    return {};
  auto remote_actor = sys_cluster.lookup_remote_named_actor(route->host,
                                                            route->port,
                                                            route->actor_name);
  if (!remote_actor)
    sys.println("[client] could not lookup {} '{}'", actor_label, node_name);
  return remote_actor;
}

void print_region_status(scoped_actor& self, actor_system& sys,
                         const actor& region_actor) {
  self->request(region_actor, 10s, region_status_atom_v).receive(
    [&](const region_snapshot& snapshot) {
      std::vector<std::string> labels;
      for (const auto& child : snapshot.children)
        labels.push_back(child.node_name + ":" + to_string(child.kind));
      sys.println("[client] region '{}' attached children [{}]",
                  snapshot.region_name, join_strings(labels));
    },
    [&](const error& err) {
      sys.println("[client] region status failed: {}", to_string(err));
    }
  );
}

} // namespace

void run_client(actor_system& sys, const node_config& cfg) {
  cluster sys_cluster(sys, cfg);
  if (!sys_cluster.connect_to_master()) {
    sys.println("[client] could not lookup master actor");
    return;
  }
  scoped_actor self{sys};
  auto topology = sys_cluster.request_topology(self);
  if (!topology)
    return;
  print_topology(sys, *topology);

  auto region_actor = resolve_remote_actor(sys_cluster, self, sys, cfg.region,
                                           k_region_router, "region actor");
  if (!region_actor)
    return;
  print_region_status(self, sys, region_actor);

  auto children = sys_cluster.request_children(self, cfg.region);
  if (!children)
    return;
  auto compute_node = children->first_child_of_kind(node_kind::compute);
  auto storage_node = children->first_child_of_kind(node_kind::storage);
  if (!compute_node || !storage_node) {
    sys.println("[client] region '{}' is missing compute or storage children",
                cfg.region);
    return;
  }

  auto compute_actor = resolve_remote_actor(sys_cluster, self, sys,
                                            compute_node->node_name,
                                            k_compute_service,
                                            "compute actor");
  auto storage_actor = resolve_remote_actor(sys_cluster, self, sys,
                                            storage_node->node_name,
                                            k_storage_service,
                                            "storage actor");
  if (!compute_actor || !storage_actor)
    return;

  self->request(compute_actor, 10s, compute_analyze_atom_v,
                analytics_request{{8, 13, 21, 34, 55}})
    .receive(
      [&](const analytics_result& result) {
        sys.println("[client] compute {} -> count={} sum={} max={}",
                    result.node_name, result.count, result.sum, result.max);
      },
      [&](const error& err) {
        sys.println("[client] compute request failed: {}", to_string(err));
      }
    );

  self->request(storage_actor, 10s, storage_lookup_atom_v,
                storage_request{cfg.storage_key})
    .receive(
      [&](const storage_result& result) {
        sys.println("[client] storage {} -> {}={}", result.node_name, result.key,
                    result.value);
      },
      [&](const error& err) {
        sys.println("[client] storage request failed: {}", to_string(err));
      }
    );
}

void caf_main(actor_system& sys, const client_config& cfg) {
  run_client(sys, cfg);
}

CAF_MAIN(id_block::distributed_nodes, io::middleman)
