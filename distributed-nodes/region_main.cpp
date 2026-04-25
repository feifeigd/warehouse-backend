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
  auto shutdown = std::make_shared<shutdown_signal>();
  auto control = sys.spawn(node_control_actor_fun, manifest, shutdown);
  auto router = sys.spawn(actor_from_state<region_state>, manifest,
                          std::chrono::seconds{cfg.lease_seconds});
  sys.registry().put(k_node_control, control);
  sys.registry().put(k_region_router, router);
  node_heartbeat heartbeats;

  do{
    if (!start_managed_node(sys, cfg, sys_cluster, manifest, control,
                            {control, router},
                            false)) {
      break;
    }
    if (!heartbeats.start(sys, sys_cluster, cfg, manifest, false)) {
      stop_managed_node(sys_cluster, manifest, {control, router}, false);
      break;
    }
    auto trigger = shutdown->wait(sys, "region", manifest.node_name, cfg.lifetime);
    heartbeats.stop();
    propagate_orderly_shutdown(sys, sys_cluster, cfg, manifest, trigger);
    stop_managed_node(sys_cluster, manifest, {control, router}, false);

  }while(false);
  
  shutdown_actors({control, router});
}

void caf_main(actor_system& sys, const region_config& cfg) {
  run_region(sys, cfg);
}

CAF_MAIN(id_block::distributed_nodes, io::middleman)
