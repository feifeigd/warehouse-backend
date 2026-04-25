#include "region_state.h"

#include <caf/caf_main.hpp>

struct region_config : node_config {
  region_config() : node_config("region-a", 47010, "master") {
    // nop
  }
};

void run_region(actor_system& sys, const node_config& cfg) {
  cluster sys_cluster(sys, cfg);
  auto manifest = make_manifest(cfg, node_kind::region);
  auto control = sys.spawn(node_control_actor_fun, manifest);
  auto router = sys.spawn(actor_from_state<region_state>, manifest);
  sys.registry().put(k_node_control, control);
  sys.registry().put(k_region_router, router);
  if (!open_node_port(sys, cfg.bind, cfg.port))
    return;
  
  if (!sys_cluster.register_with_master(manifest))
    return;
  wait_for_shutdown(sys, "region", cfg.lifetime);
  anon_send_exit(control, exit_reason::user_shutdown);
  anon_send_exit(router, exit_reason::user_shutdown);
}

void caf_main(actor_system& sys, const region_config& cfg) {
  run_region(sys, cfg);
}

CAF_MAIN(id_block::distributed_nodes, io::middleman)
