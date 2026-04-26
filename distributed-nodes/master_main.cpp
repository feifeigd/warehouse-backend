#include "master_state.h"

#include <caf/caf_main.hpp>

struct master_config : node_config {
  master_config() : node_config("master", 47000) {
    // nop
  }
};

void run_master(actor_system& sys, const node_config& cfg) {
  auto manifest = make_manifest(node_kind::master, cfg.name, cfg.host, cfg.port,
                                cfg.parent);
  auto shutdown = std::make_shared<shutdown_signal>();
  shutdown->start(sys, manifest.node_name, cfg.lifetime);
  auto control = sys.spawn(node_control_actor_fun, manifest, shutdown);
  auto master_actor = sys.spawn(actor_from_state<master_state>, manifest,
                                std::chrono::seconds{cfg.lease_seconds});
  cluster sys_cluster(sys, cfg, master_actor);
  sys.registry().put(k_node_control, control);
  sys.registry().put(k_master_control, master_actor);
  run_master_node_lifecycle(sys, cfg, sys_cluster, manifest, shutdown,
                            {control, master_actor});
}

void caf_main(actor_system& sys, const master_config& cfg) {
  run_master(sys, cfg);
}

CAF_MAIN(id_block::distributed_nodes, io::middleman)
