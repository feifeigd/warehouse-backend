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

#include "shutdown_signal.hpp"

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
  CAF_ADD_TYPE_ID(distributed_nodes, (node_manifest))
  CAF_ADD_TYPE_ID(distributed_nodes, (node_registration))
  CAF_ADD_TYPE_ID(distributed_nodes, (shutdown_request))
  CAF_ADD_TYPE_ID(distributed_nodes, (register_reply))
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

behavior node_control_actor_fun(event_based_actor* self, node_manifest manifest,
                                std::shared_ptr<shutdown_signal> signal) {
  return {
    [manifest](node_describe_atom) {
      return manifest;  // 集群端口配置
    },
    [self, manifest, signal](node_shutdown_atom,
                             shutdown_request request) -> result<register_reply> {
      const auto source = request.source;
      auto source_node = request.source_node.empty() ? "<unknown>"
                                                     : request.source_node;
      auto reason = request.reason;
      auto accepted = signal->request_shutdown(std::move(request));
      self->println("[{}:{}] shutdown {} from '{}' ({}) reason={}",
                    to_string(manifest.kind), manifest.node_name,
                    accepted ? "requested" : "already pending",
                    source_node, to_string(source), reason);
      if (source == shutdown_source::parent) {
        auto waiter = self->make_response_promise();
        signal->add_completion_waiter(waiter);
        return waiter;
      }
      return register_reply{true, accepted ? "shutdown requested"
                                           : "shutdown already pending"};
    },
  };
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


bool open_node_port(actor_system& sys, const std::string& bind_addr,
                    uint16_t port) {
  const char* listen_addr = bind_addr.empty() ? nullptr : bind_addr.c_str();
  auto actual = sys.middleman().open(port, listen_addr, true);
  if (!actual) {
    sys.println("failed to open port {}: {}", port, to_string(actual.error()));
    return false;
  }
  return true;
}

void print_tree(actor_system& sys, const topology_snapshot& snapshot,
                const std::string& parent, int depth = 0) {
  std::vector<node_manifest> children;
  for (const auto& node : snapshot.nodes) {
    if (node.parent == parent)
      children.push_back(node);
  }
  sort_manifests(children);
  for (const auto& node : children) {
    auto indent = std::string(static_cast<size_t>(depth) * 2, ' ');
    sys.println("{}- {} [{}] {}:{} actors=[{}]", indent, node.node_name,
                to_string(node.kind), node.host, node.port,
                join_strings(node.exported_actors));
    print_tree(sys, snapshot, node.node_name, depth + 1);
  }
}


struct node_config : actor_system_config {
  std::string name;
  std::string host = "127.0.0.1";
  std::string bind;
  uint16_t port;
  std::string master_host = "127.0.0.1";
  uint16_t master_port = 47000;
  std::string parent;
  std::string region = "region-a";
  std::string storage_key = "profile";
  uint32_t lease_seconds = 15;
  uint32_t node_heartbeat_seconds = 5;
  bool shutdown_parent_on_exit = false;
  bool shutdown_children_on_exit = false;
  uint32_t lifetime = 0;

  explicit node_config(std::string default_name, uint16_t default_port,
                       std::string default_parent = {},
                       std::string default_region = "region-a")
    : name(std::move(default_name)),
      port(default_port),
      parent(std::move(default_parent)),
      region(std::move(default_region)) {
    opt_group{custom_options_, "global"}
      .add(name, "name,n", "logical node name")
      .add(host, "host,H", "advertised host for the current node")
      .add(bind, "bind,B", "bind address for the local listener")
      .add(port, "port,p", "listener port for the current node")
      .add(master_host, "master-host", "master host for non-master nodes")
      .add(master_port, "master-port", "master port for non-master nodes")
      .add(parent, "parent", "parent node name for tree attachment")
      .add(region, "region,r", "target region for client mode")
      .add(storage_key, "storage-key", "lookup key for the storage node")
      .add(lease_seconds, "lease-seconds",
           "seconds before master/region evict a silent node")
      .add(node_heartbeat_seconds, "node-heartbeat-seconds",
           "seconds between node heartbeat refreshes, 0 disables heartbeats")
      .add(shutdown_parent_on_exit, "shutdown-parent-on-exit",
           "request parent shutdown when this node exits")
      .add(shutdown_children_on_exit, "shutdown-children-on-exit",
           "request child shutdown when this node exits")
      .add(lifetime, "lifetime,l",
           "node lifetime in seconds, 0 waits for <enter>");
  }
};

node_manifest make_manifest(const node_config& cfg, node_kind kind) {
  node_manifest manifest;
  manifest.kind = kind;
  manifest.node_name = cfg.name;
  manifest.host = cfg.host;
  manifest.port = cfg.port;
  manifest.parent = cfg.parent;
  manifest.exported_actors = exported_actor_names(kind);
  return manifest;
}


class cluster{
  actor_system& sys_;
  const node_config& cfg_;
  actor master_actor_;
public:
  cluster(actor_system& sys, const node_config& cfg, actor master_actor = {}) : sys_(sys), cfg_(cfg), master_actor_(master_actor) {
    // nop
  }

  bool connect_to_master() {
    master_actor_ = lookup_remote_named_actor(cfg_.master_host, cfg_.master_port,
                                            k_master_control);
    if (!master_actor_) {
      sys_.println("[{}] failed to lookup master actor at {}:{}", cfg_.name,
                  cfg_.master_host, cfg_.master_port);
      return false;
    }
    return true;
  }
  
  bool register_with_master(const node_manifest& manifest,
                            const actor& monitor_actor) {
    if(!master_actor_) {
      if(!connect_to_master())
        return false;
    }

    scoped_actor self{sys_};
    auto reply = request_register(self, node_registration{manifest, monitor_actor});
    if (!reply || !reply->ok) {
      sys_.println("[{}] registration was rejected", cfg_.name);
      return false;
    }
    sys_.println("[{}] registered with master: {}", cfg_.name, reply->message);
    return true;
  }

  actor lookup_remote_named_actor(const std::string& host,
                                  uint16_t port, const std::string& actor_name) {
    auto nid = with_retry([&] { return sys_.middleman().connect(host, port); });
    if (!nid)
      return {};
    auto actor_ptr = with_retry(
      [&] { return sys_.middleman().remote_lookup(actor_name, *nid); });
    if (!actor_ptr)
      return {};
    return actor_cast<actor>(actor_ptr);
  }

  bool attach_to_parent_region(const node_manifest& manifest,
                               const actor& monitor_actor) {
    if (manifest.parent.empty())
      return true;
    if (!master_actor_ && !connect_to_master())
      return false;
    scoped_actor self{sys_};
    auto route = request_route(self, manifest.parent, k_region_router);
    if (!route) {
      sys_.println("[{}] could not resolve parent region '{}'", manifest.node_name,
                  manifest.parent);
      return false;
    }
    auto region_actor = lookup_remote_named_actor(route->host, route->port,
                                                  route->actor_name);
    if (!region_actor) {
      sys_.println("[{}] could not lookup parent region actor '{}'",
                  manifest.node_name, manifest.parent);
      return false;
    }
    auto ok = false;
    self->request(region_actor, 10s, region_attach_atom_v,
                  node_registration{manifest, monitor_actor}).receive(
      [&](const register_reply& reply) {
        sys_.println("[{}] parent attach: {}", manifest.node_name, reply.message);
        ok = reply.ok;
      },
      [&](const error& err) {
        sys_.println("[{}] parent attach failed: {}", manifest.node_name,
                    to_string(err));
      }
    );
    return ok;
  }

  bool detach_from_parent_region(const node_manifest& manifest) {
    if (manifest.parent.empty())
      return true;
    if (!master_actor_ && !connect_to_master())
      return false;
    scoped_actor self{sys_};
    auto route = request_route(self, manifest.parent, k_region_router);
    if (!route) {
      sys_.println("[{}] could not resolve parent region '{}' for detach",
                  manifest.node_name, manifest.parent);
      return false;
    }
    auto region_actor = lookup_remote_named_actor(route->host, route->port,
                                                  route->actor_name);
    if (!region_actor) {
      sys_.println("[{}] could not lookup parent region actor '{}' for detach",
                  manifest.node_name, manifest.parent);
      return false;
    }
    auto ok = false;
    self->request(region_actor, 10s, region_detach_atom_v, manifest.node_name)
      .receive(
        [&](const register_reply& reply) {
          sys_.println("[{}] parent detach: {}", manifest.node_name, reply.message);
          ok = reply.ok;
        },
        [&](const error& err) {
          sys_.println("[{}] parent detach failed: {}", manifest.node_name,
                      to_string(err));
        }
      );
    return ok;
  }

  bool unregister_from_master(const std::string& node_name) {
    if (!master_actor_ && !connect_to_master())
      return false;
    scoped_actor self{sys_};
    auto ok = false;
    self->request(master_actor_, 10s, master_unregister_atom_v, node_name).receive(
      [&](const register_reply& reply) {
        sys_.println("[{}] unregister from master: {}", cfg_.name, reply.message);
        ok = reply.ok;
      },
      [&](const error& err) {
        sys_.println("[{}] unregister failed: {}", cfg_.name, to_string(err));
      }
    );
    return ok;
  }

  actor master_actor() const {
    return master_actor_;
  }

  actor lookup_parent_region_actor(const node_manifest& manifest) {
    if (manifest.parent.empty())
      return {};
    if (!master_actor_ && !connect_to_master())
      return {};
    scoped_actor self{sys_};
    auto route = request_route(self, manifest.parent, k_region_router);
    if (!route) {
      sys_.println("[{}] could not resolve parent region '{}' for heartbeat",
                   manifest.node_name, manifest.parent);
      return {};
    }
    auto region_actor = lookup_remote_named_actor(route->host, route->port,
                                                  route->actor_name);
    if (!region_actor) {
      sys_.println("[{}] could not lookup parent region actor '{}' for heartbeat",
                   manifest.node_name, manifest.parent);
      return {};
    }
    return region_actor;
  }

  actor lookup_node_control_actor(scoped_actor& self,
                                  const std::string& node_name) {
    auto route = request_route(self, node_name, k_node_control);
    if (!route)
      return {};
    return lookup_remote_named_actor(route->host, route->port, route->actor_name);
  }

  bool request_node_shutdown(scoped_actor& self, const std::string& node_name,
                             const shutdown_request& request) {
    auto control = lookup_node_control_actor(self, node_name);
    if (!control) {
      sys_.println("[{}] could not lookup node control for '{}'", cfg_.name,
                   node_name);
      return false;
    }
    auto ok = false;
    self->request(control, 30s, node_shutdown_atom_v, request).receive(
      [&](const register_reply& reply) {
        ok = reply.ok;
      },
      [&](const error& err) {
        sys_.println("[{}] shutdown request to '{}' failed: {}", cfg_.name,
                     node_name, to_string(err));
      }
    );
    return ok;
  }

  std::optional<topology_snapshot> request_topology(scoped_actor& self) {
    std::optional<topology_snapshot> snapshot;
    self->request(master_actor_, 10s, master_topology_atom_v).receive(
      [&](const topology_snapshot& value) {
        snapshot = value;
      },
      [&](const error& err) {
        sys_.println("[{}] topology request failed: {}", cfg_.name, to_string(err));
      }
    );
    return snapshot;
  }
  
  std::optional<actor_route> request_route(scoped_actor& self,
                                          const std::string& node_name,
                                          const std::string& actor_name) {
    std::optional<actor_route> route;
    self->request(master_actor_, 10s, master_resolve_atom_v, node_name, actor_name)
      .receive(
        [&](const actor_route& value) {
          route = value;
        },
        [&](const error& err) {
          sys_.println("[{}] resolve {}:{} failed: {}", cfg_.name, node_name, actor_name,
                        to_string(err));
        }
      );
    return route;
  }
  
  std::optional<child_snapshot> request_children(scoped_actor& self,
                                                const std::string& parent_name) {
    std::optional<child_snapshot> snapshot;
    self->request(master_actor_, 10s, master_children_atom_v, parent_name).receive(
      [&](const child_snapshot& value) {
        snapshot = value;
      },
      [&](const error& err) {
        self->println("children request failed: {}", to_string(err));
      }
    );
    return snapshot;
  }
private:

  std::optional<register_reply> request_register(scoped_actor& self,
                                                const node_registration& registration) {
    std::optional<register_reply> reply;
    self->request(master_actor_, 10s, master_register_atom_v, registration).receive(
      [&](const register_reply& value) {
        reply = value;
      },
      [&](const error& err) {
        self->println("registration failed: {}", to_string(err));
      }
    );
    return reply;
  }
};

behavior node_heartbeat_actor_fun(event_based_actor* self, actor master_actor,
                                  actor parent_actor, std::string node_name,
                                  std::chrono::seconds interval) {
  auto schedule_next = [self, interval] {
    self->delayed_send(self, interval, heartbeat_tick_atom_v);
  };
  schedule_next();
  return {
    [=](heartbeat_tick_atom) mutable {
      self->request(master_actor, 10s, master_heartbeat_atom_v, node_name).then(
        [=](const register_reply& reply) mutable {
          if (!reply.ok) {
            self->println("[{}] master heartbeat rejected: {}", node_name,
                          reply.message);
            schedule_next();
            return;
          }
          if (!parent_actor) {
            schedule_next();
            return;
          }
          self->request(parent_actor, 10s, region_heartbeat_atom_v, node_name)
            .then(
              [=](const register_reply& parent_reply) mutable {
                if (!parent_reply.ok) {
                  self->println("[{}] parent heartbeat rejected: {}", node_name,
                                parent_reply.message);
                }
                schedule_next();
              },
              [=](const error& err) mutable {
                self->println("[{}] parent heartbeat failed: {}", node_name,
                              to_string(err));
                schedule_next();
              }
            );
        },
        [=](const error& err) mutable {
          self->println("[{}] master heartbeat failed: {}", node_name,
                        to_string(err));
          schedule_next();
        }
      );
    },
  };
}

/// Sends periodic heartbeats to the parent region and master.
class node_heartbeat {
public:
  node_heartbeat() = default;

  ~node_heartbeat() {
    stop();
  }

  bool start(actor_system& sys, cluster& sys_cluster, const node_config& cfg,
             const node_manifest& manifest, bool heartbeat_parent) {
    stop();
    if (cfg.node_heartbeat_seconds == 0)
      return true; // Heartbeats disabled by config.
    auto master_actor = sys_cluster.master_actor();
    if (!master_actor) {
      if (!sys_cluster.connect_to_master())
        return false;
      master_actor = sys_cluster.master_actor();
    }
    actor parent_actor;
    if (heartbeat_parent) {
      parent_actor = sys_cluster.lookup_parent_region_actor(manifest);
      if (!parent_actor)
        return false;
    }
    worker_ = sys.spawn(node_heartbeat_actor_fun, master_actor, parent_actor,
                        manifest.node_name,
                        std::chrono::seconds{cfg.node_heartbeat_seconds});
    return static_cast<bool>(worker_);
  }

  void stop() {
    if (worker_) {
      anon_send_exit(worker_, exit_reason::user_shutdown);
      worker_ = {};
    }
  }

private:
  actor worker_;
};

shutdown_request make_local_shutdown_request(const node_manifest& manifest,
                                             const std::string& reason) {
  return shutdown_request{
    manifest.node_name,
    manifest.node_name,
    reason,
    shutdown_source::local,
  };
}

shutdown_request make_parent_shutdown_request(const node_manifest& manifest,
                                              const shutdown_request& trigger) {
  return shutdown_request{
    trigger.initiator.empty() ? manifest.node_name : trigger.initiator,
    manifest.node_name,
    trigger.reason.empty() ? "parent cascade" : trigger.reason,
    shutdown_source::child,
  };
}

shutdown_request make_child_shutdown_request(const node_manifest& manifest,
                                             const shutdown_request& trigger) {
  return shutdown_request{
    trigger.initiator.empty() ? manifest.node_name : trigger.initiator,
    manifest.node_name,
    trigger.reason.empty() ? "child cascade" : trigger.reason,
    shutdown_source::parent,
  };
}

std::vector<std::string> collect_subtree_node_names(
  const topology_snapshot& snapshot, const std::string& root_name) {
  std::vector<std::string> result;
  std::vector<std::string> pending{root_name};
  for (size_t index = 0; index < pending.size(); ++index) {
    const auto& parent = pending[index];
    for (const auto& node : snapshot.nodes) {
      if (node.parent == parent) {
        result.push_back(node.node_name);
        pending.push_back(node.node_name);
      }
    }
  }
  return result;
}

bool topology_contains_any(const topology_snapshot& snapshot,
                           const std::vector<std::string>& node_names) {
  return std::any_of(snapshot.nodes.begin(), snapshot.nodes.end(),
                     [&](const node_manifest& node) {
                       return std::find(node_names.begin(), node_names.end(),
                                        node.node_name) != node_names.end();
                     });
}

void wait_for_subtree_shutdown(actor_system& sys, cluster& sys_cluster,
                               const node_config& cfg,
                               const node_manifest& manifest,
                               const std::vector<std::string>& node_names) {
  if (node_names.empty())
    return;
  scoped_actor self{sys};
  const auto deadline = steady_clock_type::now() + 15s;
  while (steady_clock_type::now() < deadline) {
    auto topology = sys_cluster.request_topology(self);
    if (!topology)
      break;
    if (!topology_contains_any(*topology, node_names)) {
      sys.println("[{}] subtree under '{}' stopped", cfg.name,
                  manifest.node_name);
      return;
    }
    std::this_thread::sleep_for(200ms);
  }
  sys.println("[{}] timed out waiting for subtree under '{}' to stop",
              cfg.name, manifest.node_name);
}

std::vector<std::string> propagate_shutdown_to_children(
                                    actor_system& sys, cluster& sys_cluster,
                                    const node_config& cfg,
                                    const node_manifest& manifest,
                                    const shutdown_request& trigger) {
  if (!cfg.shutdown_children_on_exit)
    return {};
  scoped_actor self{sys};
  auto topology = sys_cluster.request_topology(self);
  if (!topology)
    return {};
  auto subtree_node_names = collect_subtree_node_names(*topology,
                                                       manifest.node_name);
  auto request = make_child_shutdown_request(manifest, trigger);
  auto skip_child = trigger.source == shutdown_source::child
                    ? trigger.source_node
                    : std::string{};
  for (const auto& child : topology->nodes) {
    if (child.parent != manifest.node_name)
      continue;
    if (!skip_child.empty() && child.node_name == skip_child)
      continue;
    sys_cluster.request_node_shutdown(self, child.node_name, request);
  }
  return subtree_node_names;
}

void propagate_shutdown_to_parent(actor_system& sys, cluster& sys_cluster,
                                  const node_config& cfg,
                                  const node_manifest& manifest,
                                  const shutdown_request& trigger) {
  if (!cfg.shutdown_parent_on_exit || manifest.parent.empty())
    return;
  if (trigger.source == shutdown_source::parent)
    return;
  scoped_actor self{sys};
  sys_cluster.request_node_shutdown(self, manifest.parent,
                                    make_parent_shutdown_request(manifest,
                                                                 trigger));
}

// 顺序关机：先通知子节点，再通知父节点，避免父节点先关机导致子节点无法接收关机通知。
void propagate_orderly_shutdown(actor_system& sys, cluster& sys_cluster,
                                const node_config& cfg,
                                const node_manifest& manifest,
                                const shutdown_request& trigger) {
  if (!cfg.shutdown_children_on_exit && !cfg.shutdown_parent_on_exit)
    return;
  sys.println("[{}] shutdown source={} initiator='{}' reason={}",
              manifest.node_name, to_string(trigger.source),
              trigger.initiator.empty() ? manifest.node_name : trigger.initiator,
              trigger.reason.empty() ? "<none>" : trigger.reason);
  auto subtree_node_names = propagate_shutdown_to_children(sys, sys_cluster,
                                                           cfg, manifest,
                                                           trigger);
  wait_for_subtree_shutdown(sys, sys_cluster, cfg, manifest, subtree_node_names);
}

bool start_managed_node(actor_system& sys, const node_config& cfg,
                        cluster& sys_cluster, const node_manifest& manifest,
                        const actor& monitor_actor,
                        std::initializer_list<actor> actors,
                        bool attach_parent) {
  if (!open_node_port(sys, cfg.bind, cfg.port)) {
    shutdown_actors(actors);
    return false;
  }
  if (!sys_cluster.register_with_master(manifest, monitor_actor)) {
    shutdown_actors(actors);
    return false;
  }
  if (attach_parent && !sys_cluster.attach_to_parent_region(manifest,
                                                            monitor_actor)) {
    sys_cluster.unregister_from_master(manifest.node_name);
    shutdown_actors(actors);
    return false;
  }
  return true;
}

void stop_managed_node(cluster& sys_cluster, const node_manifest& manifest,
                       std::initializer_list<actor> actors,
                       bool attach_parent) {
  if (attach_parent)
    sys_cluster.detach_from_parent_region(manifest);
  sys_cluster.unregister_from_master(manifest.node_name);
  shutdown_actors(actors);
}
