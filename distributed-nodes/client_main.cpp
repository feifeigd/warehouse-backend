#include "app.hpp"

#include <caf/caf_main.hpp>

struct client_config : node_config {
  client_config() : node_config("client", 0, {}, "region-a") {
    // nop
  }
};

void caf_main(actor_system& sys, const client_config& cfg) {
  run_client(sys, cfg);
}

CAF_MAIN(id_block::distributed_nodes, io::middleman)
