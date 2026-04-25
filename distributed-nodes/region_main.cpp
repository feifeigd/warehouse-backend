#include "app.hpp"

#include <caf/caf_main.hpp>

struct region_config : node_config {
  region_config() : node_config("region-a", 46010, "master") {
    // nop
  }
};

void caf_main(actor_system& sys, const region_config& cfg) {
  run_region(sys, cfg);
}

CAF_MAIN(id_block::distributed_nodes, io::middleman)
