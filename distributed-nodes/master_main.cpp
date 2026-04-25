#include "app.hpp"

#include <caf/caf_main.hpp>

struct master_config : node_config {
  master_config() : node_config("master", 47000) {
    // nop
  }
};

void caf_main(actor_system& sys, const master_config& cfg) {
  run_master(sys, cfg);
}

CAF_MAIN(id_block::distributed_nodes, io::middleman)
