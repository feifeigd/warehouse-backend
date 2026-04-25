#include "master_state.h"

#include <caf/caf_main.hpp>

struct master_config : node_config {
  master_config() : node_config("master", 47000) {
    // nop
  }
};

void run_master(actor_system& sys, const node_config& cfg) {
  auto manifest = make_manifest(cfg, node_kind::master);
  auto master_actor = sys.spawn(actor_from_state<master_state>, manifest);
  sys.registry().put(k_master_control, master_actor);
  if (!open_node_port(sys, cfg.bind, cfg.port))
    return;
  sys.println("[master] '{}' listening on {}:{}", cfg.name, cfg.host, cfg.port);
  wait_for_shutdown(sys, "master", cfg.lifetime);
  anon_send_exit(master_actor, exit_reason::user_shutdown);
}

void caf_main(actor_system& sys, const master_config& cfg) {
  run_master(sys, cfg);
}

CAF_MAIN(id_block::distributed_nodes, io::middleman)
