#pragma once

#include <caf/actor_from_state.hpp>
#include <caf/actor_registry.hpp>
#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/default_enum_inspect.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/io/middleman.hpp>
#include <caf/result.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/type_id.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace caf;
using namespace std::chrono_literals;

enum class node_kind : uint8_t {
  master = 0,
  region,
  compute,
  storage,
};

enum class shutdown_source : uint8_t {
  local = 0,
  parent,
  child,
  external,
};

enum class rpc_error_code : uint8_t {
  none = 0,
  master_unavailable,
  service_not_registered,
  service_unavailable,
  resolve_failed,
  request_timeout,
  request_failed,
};

std::string to_string(node_kind x) {
  switch (x) {
    case node_kind::master:
      return "master";
    case node_kind::region:
      return "region";
    case node_kind::compute:
      return "compute";
    case node_kind::storage:
      return "storage";
  }
  return "unknown";
}

bool from_string(std::string_view str, node_kind& x) {
  if (str == "master") {
    x = node_kind::master;
    return true;
  }
  if (str == "region") {
    x = node_kind::region;
    return true;
  }
  if (str == "compute") {
    x = node_kind::compute;
    return true;
  }
  if (str == "storage") {
    x = node_kind::storage;
    return true;
  }
  return false;
}

bool from_integer(uint8_t value, node_kind& x) {
  switch (value) {
    case 0:
      x = node_kind::master;
      return true;
    case 1:
      x = node_kind::region;
      return true;
    case 2:
      x = node_kind::compute;
      return true;
    case 3:
      x = node_kind::storage;
      return true;
    default:
      return false;
  }
}

template <class Inspector>
bool inspect(Inspector& f, node_kind& x) {
  return caf::default_enum_inspect(f, x);
}

std::string to_string(shutdown_source x) {
  switch (x) {
    case shutdown_source::local:
      return "local";
    case shutdown_source::parent:
      return "parent";
    case shutdown_source::child:
      return "child";
    case shutdown_source::external:
      return "external";
  }
  return "unknown";
}

bool from_string(std::string_view str, shutdown_source& x) {
  if (str == "local") {
    x = shutdown_source::local;
    return true;
  }
  if (str == "parent") {
    x = shutdown_source::parent;
    return true;
  }
  if (str == "child") {
    x = shutdown_source::child;
    return true;
  }
  if (str == "external") {
    x = shutdown_source::external;
    return true;
  }
  return false;
}

bool from_integer(uint8_t value, shutdown_source& x) {
  switch (value) {
    case 0:
      x = shutdown_source::local;
      return true;
    case 1:
      x = shutdown_source::parent;
      return true;
    case 2:
      x = shutdown_source::child;
      return true;
    case 3:
      x = shutdown_source::external;
      return true;
    default:
      return false;
  }
}

template <class Inspector>
bool inspect(Inspector& f, shutdown_source& x) {
  return caf::default_enum_inspect(f, x);
}

std::string to_string(rpc_error_code x) {
  switch (x) {
    case rpc_error_code::none:
      return "none";
    case rpc_error_code::master_unavailable:
      return "master_unavailable";
    case rpc_error_code::service_not_registered:
      return "service_not_registered";
    case rpc_error_code::service_unavailable:
      return "service_unavailable";
    case rpc_error_code::resolve_failed:
      return "resolve_failed";
    case rpc_error_code::request_timeout:
      return "request_timeout";
    case rpc_error_code::request_failed:
      return "request_failed";
  }
  return "unknown";
}

bool from_string(std::string_view str, rpc_error_code& x) {
  if (str == "none") {
    x = rpc_error_code::none;
    return true;
  }
  if (str == "master_unavailable") {
    x = rpc_error_code::master_unavailable;
    return true;
  }
  if (str == "service_not_registered") {
    x = rpc_error_code::service_not_registered;
    return true;
  }
  if (str == "service_unavailable") {
    x = rpc_error_code::service_unavailable;
    return true;
  }
  if (str == "resolve_failed") {
    x = rpc_error_code::resolve_failed;
    return true;
  }
  if (str == "request_timeout") {
    x = rpc_error_code::request_timeout;
    return true;
  }
  if (str == "request_failed") {
    x = rpc_error_code::request_failed;
    return true;
  }
  return false;
}

bool from_integer(uint8_t value, rpc_error_code& x) {
  switch (value) {
    case 0:
      x = rpc_error_code::none;
      return true;
    case 1:
      x = rpc_error_code::master_unavailable;
      return true;
    case 2:
      x = rpc_error_code::service_not_registered;
      return true;
    case 3:
      x = rpc_error_code::service_unavailable;
      return true;
    case 4:
      x = rpc_error_code::resolve_failed;
      return true;
    case 5:
      x = rpc_error_code::request_timeout;
      return true;
    case 6:
      x = rpc_error_code::request_failed;
      return true;
    default:
      return false;
  }
}

template <class Inspector>
bool inspect(Inspector& f, rpc_error_code& x) {
  return caf::default_enum_inspect(f, x);
}

struct node_manifest {
  node_kind kind = node_kind::master;
  std::string node_name;
  std::string host;
  uint16_t port = 0;
  std::string parent;
  std::vector<std::string> exported_actors;
};

template <class Inspector>
bool inspect(Inspector& f, node_manifest& x) {
  return f.object(x).fields(
    f.field("kind", x.kind),
    f.field("node_name", x.node_name),
    f.field("host", x.host),
    f.field("port", x.port),
    f.field("parent", x.parent),
    f.field("exported_actors", x.exported_actors)
  );
}

struct node_registration {
  node_manifest manifest;
  actor monitor_actor;
};

template <class Inspector>
bool inspect(Inspector& f, node_registration& x) {
  return f.object(x).fields(
    f.field("manifest", x.manifest),
    f.field("monitor_actor", x.monitor_actor)
  );
}

struct shutdown_request {
  std::string initiator;
  std::string source_node;
  std::string reason;
  shutdown_source source = shutdown_source::local;
};

template <class Inspector>
bool inspect(Inspector& f, shutdown_request& x) {
  return f.object(x).fields(
    f.field("initiator", x.initiator),
    f.field("source_node", x.source_node),
    f.field("reason", x.reason),
    f.field("source", x.source)
  );
}

struct register_reply {
  bool ok = false;
  std::string message;
};

template <class Inspector>
bool inspect(Inspector& f, register_reply& x) {
  return f.object(x).fields(
    f.field("ok", x.ok),
    f.field("message", x.message)
  );
}

struct rpc_actor_result {
  bool ok = false;
  actor remote;
  rpc_error_code code = rpc_error_code::none;
  std::string message;
};

template <class Inspector>
bool inspect(Inspector& f, rpc_actor_result& x) {
  return f.object(x).fields(
    f.field("ok", x.ok),
    f.field("remote", x.remote),
    f.field("code", x.code),
    f.field("message", x.message)
  );
}


struct actor_route {
  std::string node_name;
  node_kind kind = node_kind::master;
  std::string host;
  uint16_t port = 0;
  std::string actor_name;
  std::string parent;
};

template <class Inspector>
bool inspect(Inspector& f, actor_route& x) {
  return f.object(x).fields(
    f.field("node_name", x.node_name),
    f.field("kind", x.kind),
    f.field("host", x.host),
    f.field("port", x.port),
    f.field("actor_name", x.actor_name),
    f.field("parent", x.parent)
  );
}

struct topology_snapshot {
  std::vector<node_manifest> nodes;
};

template <class Inspector>
bool inspect(Inspector& f, topology_snapshot& x) {
  return f.object(x).fields(
    f.field("nodes", x.nodes)
  );
}

struct child_snapshot {
  std::string parent;
  std::vector<node_manifest> children;
  
  std::optional<node_manifest> first_child_of_kind(node_kind kind) const {
    for (const auto& child : children) {
      if (child.kind == kind)
        return child;
    }
    return std::nullopt;
  }
};

template <class Inspector>
bool inspect(Inspector& f, child_snapshot& x) {
  return f.object(x).fields(
    f.field("parent", x.parent),
    f.field("children", x.children)
  );
}

struct region_snapshot {
  std::string region_name;
  std::vector<node_manifest> children;
};

template <class Inspector>
bool inspect(Inspector& f, region_snapshot& x) {
  return f.object(x).fields(
    f.field("region_name", x.region_name),
    f.field("children", x.children)
  );
}

struct analytics_request {
  std::vector<int32_t> values;
};

template <class Inspector>
bool inspect(Inspector& f, analytics_request& x) {
  return f.object(x).fields(
    f.field("values", x.values)
  );
}

struct analytics_result {
  std::string node_name;
  int64_t sum = 0;
  int32_t max = 0;
  uint32_t count = 0;
};

template <class Inspector>
bool inspect(Inspector& f, analytics_result& x) {
  return f.object(x).fields(
    f.field("node_name", x.node_name),
    f.field("sum", x.sum),
    f.field("max", x.max),
    f.field("count", x.count)
  );
}

struct storage_request {
  std::string key;
};

template <class Inspector>
bool inspect(Inspector& f, storage_request& x) {
  return f.object(x).fields(
    f.field("key", x.key)
  );
}

struct storage_result {
  std::string node_name;
  std::string key;
  std::string value;
};

template <class Inspector>
bool inspect(Inspector& f, storage_result& x) {
  return f.object(x).fields(
    f.field("node_name", x.node_name),
    f.field("key", x.key),
    f.field("value", x.value)
  );
}

CAF_BEGIN_TYPE_ID_BLOCK(distributed_nodes, first_custom_type_id)

  CAF_ADD_TYPE_ID(distributed_nodes, (node_kind))
  CAF_ADD_TYPE_ID(distributed_nodes, (shutdown_source))
  CAF_ADD_TYPE_ID(distributed_nodes, (rpc_error_code))
  CAF_ADD_TYPE_ID(distributed_nodes, (node_manifest))
  CAF_ADD_TYPE_ID(distributed_nodes, (node_registration))
  CAF_ADD_TYPE_ID(distributed_nodes, (shutdown_request))
  CAF_ADD_TYPE_ID(distributed_nodes, (register_reply))
  CAF_ADD_TYPE_ID(distributed_nodes, (rpc_actor_result))
  CAF_ADD_TYPE_ID(distributed_nodes, (actor_route))
  CAF_ADD_TYPE_ID(distributed_nodes, (topology_snapshot))
  CAF_ADD_TYPE_ID(distributed_nodes, (child_snapshot))
  CAF_ADD_TYPE_ID(distributed_nodes, (region_snapshot))
  CAF_ADD_TYPE_ID(distributed_nodes, (analytics_request))
  CAF_ADD_TYPE_ID(distributed_nodes, (analytics_result))
  CAF_ADD_TYPE_ID(distributed_nodes, (storage_request))
  CAF_ADD_TYPE_ID(distributed_nodes, (storage_result))

  CAF_ADD_ATOM(distributed_nodes, master_register_atom)
  CAF_ADD_ATOM(distributed_nodes, master_heartbeat_atom)
  CAF_ADD_ATOM(distributed_nodes, master_unregister_atom)
  CAF_ADD_ATOM(distributed_nodes, master_topology_atom)
  CAF_ADD_ATOM(distributed_nodes, master_resolve_atom)
  CAF_ADD_ATOM(distributed_nodes, master_children_atom)
  CAF_ADD_ATOM(distributed_nodes, maintenance_tick_atom)
  CAF_ADD_ATOM(distributed_nodes, heartbeat_tick_atom)
  CAF_ADD_ATOM(distributed_nodes, node_describe_atom)
  CAF_ADD_ATOM(distributed_nodes, node_shutdown_atom)
  CAF_ADD_ATOM(distributed_nodes, region_attach_atom)
  CAF_ADD_ATOM(distributed_nodes, region_heartbeat_atom)
  CAF_ADD_ATOM(distributed_nodes, region_detach_atom)
  CAF_ADD_ATOM(distributed_nodes, region_status_atom)
  CAF_ADD_ATOM(distributed_nodes, compute_analyze_atom)
  CAF_ADD_ATOM(distributed_nodes, storage_lookup_atom)
  CAF_ADD_ATOM(distributed_nodes, rpc_resolve_actor_atom)
  CAF_ADD_ATOM(distributed_nodes, rpc_invalidate_actor_atom)
  CAF_ADD_ATOM(distributed_nodes, shutdown_signal_request_atom)
  CAF_ADD_ATOM(distributed_nodes, shutdown_signal_wait_atom)
  CAF_ADD_ATOM(distributed_nodes, shutdown_signal_poll_atom)
  CAF_ADD_ATOM(distributed_nodes, shutdown_signal_lifetime_atom)

CAF_END_TYPE_ID_BLOCK(distributed_nodes)

constexpr char k_master_control[]  = "master.control";
constexpr char k_node_control[]    = "node.control";
constexpr char k_region_router[]   = "region.router";
constexpr char k_compute_service[] = "compute.service";
constexpr char k_storage_service[] = "storage.service";
constexpr auto k_maintenance_step  = 1s;

// Returns the list of actor names that a node of the given kind exports.
std::vector<std::string> exported_actor_names(node_kind kind) {
  switch (kind) {
    case node_kind::master:
      return {k_master_control, k_node_control};
    case node_kind::region:
      return {k_node_control, k_region_router};
    case node_kind::compute:
      return {k_node_control, k_compute_service};
    case node_kind::storage:
      return {k_node_control, k_storage_service};
  }
  return {};
}

std::string join_strings(const std::vector<std::string>& xs) {
  if (xs.empty())
    return "<none>";
  auto result = xs.front();
  for (size_t index = 1; index < xs.size(); ++index) {
    result += ", ";
    result += xs[index];
  }
  return result;
}

void shutdown_actors(std::initializer_list<actor> xs) {
  for (const auto& x : xs) {
    if (x)
      anon_send_exit(x, exit_reason::user_shutdown);
  }
}

using steady_clock_type = std::chrono::steady_clock;

steady_clock_type::time_point never_expires() {
  return steady_clock_type::time_point::max();
}

steady_clock_type::time_point lease_deadline(std::chrono::seconds ttl) {
  return steady_clock_type::now() + ttl;
}

template <class T>
void sort_manifests(std::vector<T>& xs) {
  std::sort(xs.begin(), xs.end(), [](const auto& lhs, const auto& rhs) {
    if (lhs.parent != rhs.parent)
      return lhs.parent < rhs.parent;
    if (lhs.kind != rhs.kind)
      return static_cast<uint8_t>(lhs.kind) < static_cast<uint8_t>(rhs.kind);
    return lhs.node_name < rhs.node_name;
  });
}

template <class F>
auto with_retry(F fn, std::chrono::milliseconds total = 4s,
                std::chrono::milliseconds step = 100ms) {
  auto result = fn();
  auto waited = 0ms;
  while (!result && waited < total) {
    std::this_thread::sleep_for(step);
    waited += step;
    result = fn();
  }
  return result;
}

actor lookup_remote_actor(actor_system& sys, const std::string& host,
                          uint16_t port, const std::string& actor_name,
                          std::chrono::milliseconds total = 4s,
                          std::chrono::milliseconds step = 100ms) {
  auto nid = with_retry([&] { return sys.middleman().connect(host, port); },
                        total, step);
  if (!nid)
    return {};
  auto actor_ptr = with_retry(
    [&] { return sys.middleman().remote_lookup(actor_name, *nid); },
    total, step);
  if (!actor_ptr)
    return {};
  return actor_cast<actor>(actor_ptr);
}

