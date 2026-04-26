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
  auto rpc_client = spawn_rpc_client(sys, cfg, sys_cluster.master_actor());

  auto region_actor = rpc_resolve_actor(self, rpc_client, cfg.region,
                                        k_region_router);
  if (!region_actor) {
    sys.println("[client] could not lookup region actor '{}'", cfg.region);
    return;
  }
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

  auto compute_result = rpc_compute_analyze(self, rpc_client,
                                            compute_node->node_name,
                                            analytics_request{{8, 13, 21, 34,
                                                              55}});
  if (compute_result) {
    sys.println("[client] compute {} -> count={} sum={} max={}",
                compute_result->node_name, compute_result->count,
                compute_result->sum, compute_result->max);
  } else {
    sys.println("[client] compute request failed");
  }

  auto storage_result = rpc_storage_lookup(self, rpc_client,
                                           storage_node->node_name,
                                           storage_request{cfg.storage_key});
  if (storage_result) {
    sys.println("[client] storage {} -> {}={}", storage_result->node_name,
                storage_result->key, storage_result->value);
  } else {
    sys.println("[client] storage request failed");
  }
}

void caf_main(actor_system& sys, const client_config& cfg) {
  run_client(sys, cfg);
}

CAF_MAIN(id_block::distributed_nodes, io::middleman)
