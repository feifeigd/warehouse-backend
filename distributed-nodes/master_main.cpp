#include "master_state.h"

#include <caf/caf_main.hpp>

struct master_config : node_config {
  master_config() : node_config("master", 47000) {
    // nop
  }
};

void run_master(actor_system& sys, const node_config& cfg) {
  auto manifest = make_manifest(cfg, node_kind::master);
  auto shutdown = std::make_shared<shutdown_signal>();
  auto control = sys.spawn(node_control_actor_fun, manifest, shutdown);
  auto master_actor = sys.spawn(actor_from_state<master_state>, manifest,
                                std::chrono::seconds{cfg.lease_seconds});
  cluster sys_cluster(sys, cfg, master_actor);
  sys.registry().put(k_node_control, control);
  sys.registry().put(k_master_control, master_actor);
  if (!open_node_port(sys, cfg.bind, cfg.port)) {
    shutdown_actors({control, master_actor});
    return;
  }
  sys.println("[master] '{}' listening on {}:{}", cfg.name, cfg.host, cfg.port);
  auto trigger = shutdown->wait(sys, "master", manifest.node_name, cfg.lifetime);
  propagate_orderly_shutdown(sys, sys_cluster, cfg, manifest, trigger);
  shutdown->complete_shutdown(register_reply{true, "shutdown complete"});
  shutdown_actors({control, master_actor});
}

void caf_main(actor_system& sys, const master_config& cfg) {
  run_master(sys, cfg);
}

CAF_MAIN(id_block::distributed_nodes, io::middleman)
