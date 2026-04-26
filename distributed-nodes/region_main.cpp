#include "region_state.h"

#include <caf/caf_main.hpp>

struct region_config : node_config {
  region_config() : node_config("region-a", 47010, "master") {
    // nop
  }
};

void run_region(actor_system& sys, const node_config& cfg) {
  cluster sys_cluster(sys, cfg);
  auto manifest = make_manifest(node_kind::region, cfg.name, cfg.host, cfg.port,
                                cfg.parent);
  auto shutdown = std::make_shared<shutdown_signal>();
  shutdown->start(sys, manifest.node_name, cfg.lifetime);
  auto control = sys.spawn(node_control_actor_fun, manifest, shutdown);
  auto router = sys.spawn(actor_from_state<region_state>, manifest,
                          std::chrono::seconds{cfg.lease_seconds});
  sys.registry().put(k_node_control, control);
  sys.registry().put(k_region_router, router);
  run_managed_node_lifecycle(sys, cfg, sys_cluster, manifest, shutdown, control,
                             {control, router}, false, false, "region");
}

void caf_main(actor_system& sys, const region_config& cfg) {
  run_region(sys, cfg);
}

CAF_MAIN(id_block::distributed_nodes, io::middleman)
