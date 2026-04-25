#include "app.hpp"

#include <caf/caf_main.hpp>

struct client_config : node_config {
  client_config() : node_config("client", 0, {}, "region-a") {
    // nop
  }
};

void run_client(actor_system& sys, const node_config& cfg) {
  auto master_actor = lookup_remote_named_actor(sys, cfg.master_host,
                                                cfg.master_port,
                                                k_master_control);
  if (!master_actor) {
    sys.println("[client] could not lookup master actor");
    return;
  }
  scoped_actor self{sys};
  auto topology = request_topology(self, master_actor);
  if (!topology)
    return;
  sys.println("[client] topology");
  for (const auto& node : topology->nodes) {
    if (node.kind == node_kind::master) {
      sys.println("- {} [{}] {}:{}", node.node_name, to_string(node.kind),
                  node.host, node.port);
      print_tree(sys, *topology, node.node_name, 1);
    }
  }
  auto region_route = request_route(self, master_actor, cfg.region, k_region_router);
  if (!region_route)
    return;
  auto region_actor = lookup_remote_named_actor(sys, region_route->host,
                                                region_route->port,
                                                region_route->actor_name);
  if (!region_actor) {
    sys.println("[client] could not lookup region actor '{}'", cfg.region);
    return;
  }
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

  auto children = request_children(self, master_actor, cfg.region);
  if (!children)
    return;
  auto compute_node = first_child_of_kind(*children, node_kind::compute);
  auto storage_node = first_child_of_kind(*children, node_kind::storage);
  if (!compute_node || !storage_node) {
    sys.println("[client] region '{}' is missing compute or storage children",
                cfg.region);
    return;
  }

  auto compute_route = request_route(self, master_actor, compute_node->node_name,
                                     k_compute_service);
  auto storage_route = request_route(self, master_actor, storage_node->node_name,
                                     k_storage_service);
  if (!compute_route || !storage_route)
    return;

  auto compute_actor = lookup_remote_named_actor(sys, compute_route->host,
                                                 compute_route->port,
                                                 compute_route->actor_name);
  auto storage_actor = lookup_remote_named_actor(sys, storage_route->host,
                                                 storage_route->port,
                                                 storage_route->actor_name);
  if (!compute_actor || !storage_actor) {
    sys.println("[client] failed to lookup compute or storage actor");
    return;
  }

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
