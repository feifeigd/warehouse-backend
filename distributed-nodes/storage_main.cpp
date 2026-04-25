#include "app.hpp"

#include <caf/caf_main.hpp>

struct storage_config : node_config {
  storage_config() : node_config("storage-a1", 47012, "region-a") {
    // nop
  }
};


behavior storage_service_actor_fun(event_based_actor* self,
                                   const node_manifest& manifest) {
  const auto parent_name = manifest.parent.empty() ? "master" : manifest.parent;
  const std::map<std::string, std::string> records{
    {"profile", "profile owned by " + manifest.node_name},
    {"parent", parent_name},
    {"tree", parent_name + " -> " + manifest.node_name},
    {"motd", "hello from " + manifest.node_name},
  };
  return {
    [self, manifest, records](storage_lookup_atom,
                              const storage_request& request) -> storage_result {
      storage_result result;
      result.node_name = manifest.node_name;
      result.key = request.key;
      if (auto iter = records.find(request.key); iter != records.end())
        result.value = iter->second;
      else
        result.value = "<missing:" + request.key + ">";
      self->println("[storage:{}] served key '{}'", manifest.node_name,
                    request.key);
      return result;
    },
  };
}

void run_storage(actor_system& sys, const node_config& cfg) {  
  cluster sys_cluster(sys, cfg);
  auto manifest = make_manifest(cfg, node_kind::storage);
  auto shutdown = std::make_shared<shutdown_signal>();
  auto control = sys.spawn(node_control_actor_fun, manifest, shutdown);
  auto service = sys.spawn(storage_service_actor_fun, manifest);
  sys.registry().put(k_node_control, control);
  sys.registry().put(k_storage_service, service);
  node_heartbeat heartbeats;

  do{
    if (!start_managed_node(sys, cfg, sys_cluster, manifest, control,
                            {control, service},
                            true)) {
      break;
    }
    if (!heartbeats.start(sys, sys_cluster, cfg, manifest, true)) {
      stop_managed_node(sys_cluster, manifest, {control, service}, true);
      break;
    }
    auto trigger = shutdown->wait(sys, "storage", manifest.node_name, cfg.lifetime);
    heartbeats.stop();
    propagate_orderly_shutdown(sys, sys_cluster, cfg, manifest, trigger);
    stop_managed_node(sys_cluster, manifest, {control, service}, true);
    shutdown->complete_shutdown(register_reply{true, "shutdown complete"});
    propagate_shutdown_to_parent(sys, sys_cluster, cfg, manifest, trigger);

  }while(false);
  
  shutdown_actors({control, service});
}

void caf_main(actor_system& sys, const storage_config& cfg) {
  run_storage(sys, cfg);
}

CAF_MAIN(id_block::distributed_nodes, io::middleman)
