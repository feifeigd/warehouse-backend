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
  CAF_ADD_TYPE_ID(distributed_nodes, (node_manifest))
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
  CAF_ADD_ATOM(distributed_nodes, master_topology_atom)
  CAF_ADD_ATOM(distributed_nodes, master_resolve_atom)
  CAF_ADD_ATOM(distributed_nodes, master_children_atom)
  CAF_ADD_ATOM(distributed_nodes, node_describe_atom)
  CAF_ADD_ATOM(distributed_nodes, region_attach_atom)
  CAF_ADD_ATOM(distributed_nodes, region_status_atom)
  CAF_ADD_ATOM(distributed_nodes, compute_analyze_atom)
  CAF_ADD_ATOM(distributed_nodes, storage_lookup_atom)

CAF_END_TYPE_ID_BLOCK(distributed_nodes)

constexpr char k_master_control[]  = "master.control";
constexpr char k_node_control[]    = "node.control";
constexpr char k_region_router[]   = "region.router";
constexpr char k_compute_service[] = "compute.service";
constexpr char k_storage_service[] = "storage.service";

// Returns the list of actor names that a node of the given kind exports.
std::vector<std::string> exported_actor_names(node_kind kind) {
  switch (kind) {
    case node_kind::master:
      return {k_master_control};
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

behavior node_control_actor_fun(node_manifest manifest) {
  return {
    [manifest](node_describe_atom) {
      return manifest;
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

void wait_for_shutdown(actor_system& sys, const std::string& role,
                       uint32_t lifetime) {
  if (lifetime > 0) {
    sys.println("[{}] running for {} seconds", role, lifetime);
    std::this_thread::sleep_for(std::chrono::seconds{lifetime});
  } else {
    sys.println("[{}] press <enter> to stop", role);
    std::string dummy;
    std::getline(std::cin, dummy);
  }
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
  
  bool register_with_master(const node_manifest& manifest) {
    if(!master_actor_) {
      if(!connect_to_master())
        return false;
    }

    scoped_actor self{sys_};
    auto reply = request_register(self, manifest);
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

  void attach_to_parent_region(const node_manifest& manifest) {
    if (manifest.parent.empty())
      return;
    scoped_actor self{sys_};
    auto route = request_route(self, manifest.parent, k_region_router);
    if (!route) {
      sys_.println("[{}] could not resolve parent region '{}'", manifest.node_name,
                  manifest.parent);
      return;
    }
    auto region_actor = lookup_remote_named_actor(route->host, route->port,
                                                  route->actor_name);
    if (!region_actor) {
      sys_.println("[{}] could not lookup parent region actor '{}'",
                  manifest.node_name, manifest.parent);
      return;
    }
    self->request(region_actor, 10s, region_attach_atom_v, manifest).receive(
      [&](const register_reply& reply) {
        sys_.println("[{}] parent attach: {}", manifest.node_name, reply.message);
      },
      [&](const error& err) {
        sys_.println("[{}] parent attach failed: {}", manifest.node_name,
                    to_string(err));
      }
    );
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
                                                const node_manifest& manifest) {
    std::optional<register_reply> reply;
    self->request(master_actor_, 10s, master_register_atom_v, manifest).receive(
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
