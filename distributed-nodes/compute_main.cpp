#include "app.hpp"

#include <caf/caf_main.hpp>

struct compute_config : node_config {
  compute_config() : node_config("compute-a1", 46011, "region-a") {
    // nop
  }
};

void caf_main(actor_system& sys, const compute_config& cfg) {
  run_compute(sys, cfg);
}

CAF_MAIN(id_block::distributed_nodes, io::middleman)
