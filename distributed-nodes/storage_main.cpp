#include "app.hpp"

#include <caf/caf_main.hpp>

struct storage_config : node_config {
  storage_config() : node_config("storage-a1", 46012, "region-a") {
    // nop
  }
};

void caf_main(actor_system& sys, const storage_config& cfg) {
  run_storage(sys, cfg);
}

CAF_MAIN(id_block::distributed_nodes, io::middleman)
