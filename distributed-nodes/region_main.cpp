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

  do{
    if (!open_node_port(sys, cfg.bind, cfg.port)) {
      break;
    }
    
    if (!sys_cluster.register_with_master(manifest)) {
      break;
    }
    
    wait_for_shutdown(sys, "region", cfg.lifetime);
    sys_cluster.unregister_from_master(manifest.node_name);

  }while(false);
  
  shutdown_actors({control, router});
}

void caf_main(actor_system& sys, const region_config& cfg) {
  run_region(sys, cfg);
}

CAF_MAIN(id_block::distributed_nodes, io::middleman)
