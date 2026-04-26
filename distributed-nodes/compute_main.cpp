#include "app.hpp"

#include <caf/caf_main.hpp>

struct compute_config : node_config {
  compute_config() : node_config("compute-a1", 47011, "region-a") {
    // nop
  }
};


behavior compute_service_actor_fun(event_based_actor* self,
                                   const node_manifest& manifest) {
  return {
    [self, manifest](compute_analyze_atom,
                     const analytics_request& request) -> analytics_result {
      analytics_result result;
      result.node_name = manifest.node_name;
      result.count = static_cast<uint32_t>(request.values.size());
      if (!request.values.empty())
        result.max = *std::max_element(request.values.begin(),
                                       request.values.end());
      for (auto value : request.values)
        result.sum += value;
      self->println("[compute:{}] processed {} values", manifest.node_name,
                    result.count);
      return result;
    },
  };
}

void run_compute(actor_system& sys, const node_config& cfg) {
  cluster sys_cluster(sys, cfg);
  auto manifest = make_manifest(cfg, node_kind::compute);
  auto shutdown = std::make_shared<shutdown_signal>();
  auto control = sys.spawn(node_control_actor_fun, manifest, shutdown);
  auto service = sys.spawn(compute_service_actor_fun, manifest);
  sys.registry().put(k_node_control, control);
  sys.registry().put(k_compute_service, service);
  run_managed_node_lifecycle(sys, cfg, sys_cluster, manifest, shutdown, control,
                             {control, service}, true, true, "compute");
}

void caf_main(actor_system& sys, const compute_config& cfg) {
  run_compute(sys, cfg);
}

CAF_MAIN(id_block::distributed_nodes, io::middleman)
