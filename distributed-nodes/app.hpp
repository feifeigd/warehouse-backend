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

constexpr char k_master_control[] = "master.control";
constexpr char k_node_control[] = "node.control";
constexpr char k_region_router[] = "region.router";
constexpr char k_compute_service[] = "compute.service";
constexpr char k_storage_service[] = "storage.service";

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

struct master_state {
  explicit master_state(event_based_actor* selfptr, node_manifest manifest)
    : self(selfptr) {
    nodes.emplace(manifest.node_name, std::move(manifest));
  }

  topology_snapshot make_topology() const {
    topology_snapshot snapshot;
    snapshot.nodes.reserve(nodes.size());
    for (const auto& [_, node] : nodes)
      snapshot.nodes.push_back(node);
    sort_manifests(snapshot.nodes);
    return snapshot;
  }

  child_snapshot make_children(const std::string& parent_name) const {
    child_snapshot snapshot;
    snapshot.parent = parent_name;
    for (const auto& [_, node] : nodes) {
      if (node.parent == parent_name)
        snapshot.children.push_back(node);
    }
    sort_manifests(snapshot.children);
    return snapshot;
  }

  behavior make_behavior() {
    return {
      [this](master_register_atom, node_manifest manifest) {
        auto existed = nodes.find(manifest.node_name) != nodes.end();
        nodes[manifest.node_name] = manifest;
        self->println("[master] {} node '{}' ({}) parent={} actors=[{}]",
                      existed ? "updated" : "registered", manifest.node_name,
                      to_string(manifest.kind),
                      manifest.parent.empty() ? "<root>" : manifest.parent,
                      join_strings(manifest.exported_actors));
        return register_reply{true, existed ? "node updated" : "node registered"};
      },
      [this](master_topology_atom) {
        return make_topology();
      },
      [this](master_children_atom, const std::string& parent_name) {
        return make_children(parent_name);
      },
      [this](master_resolve_atom, const std::string& node_name,
             const std::string& actor_name) -> result<actor_route> {
        auto iter = nodes.find(node_name);
        if (iter == nodes.end())
          return make_error(sec::no_such_key);
        const auto& manifest = iter->second;
        auto found = std::find(manifest.exported_actors.begin(),
                               manifest.exported_actors.end(), actor_name);
        if (found == manifest.exported_actors.end())
          return make_error(sec::no_such_key);
        return actor_route{
          manifest.node_name,
          manifest.kind,
          manifest.host,
          manifest.port,
          actor_name,
          manifest.parent,
        };
      },
    };
  }

  event_based_actor* self;
  std::unordered_map<std::string, node_manifest> nodes;
};

struct region_state {
  explicit region_state(event_based_actor* selfptr, node_manifest manifest)
    : self(selfptr), info(std::move(manifest)) {
    // nop
  }

  region_snapshot make_snapshot() const {
    region_snapshot snapshot;
    snapshot.region_name = info.node_name;
    for (const auto& [_, child] : children)
      snapshot.children.push_back(child);
    sort_manifests(snapshot.children);
    return snapshot;
  }

  behavior make_behavior() {
    return {
      [this](node_describe_atom) {
        return info;
      },
      [this](region_attach_atom, node_manifest child) {
        auto existed = children.find(child.node_name) != children.end();
        children[child.node_name] = child;
        self->println("[region:{}] {} child '{}' ({})", info.node_name,
                      existed ? "updated" : "attached", child.node_name,
                      to_string(child.kind));
        return register_reply{true, existed ? "child updated" : "child attached"};
      },
      [this](region_status_atom) {
        return make_snapshot();
      },
    };
  }

  event_based_actor* self;
  node_manifest info;
  std::unordered_map<std::string, node_manifest> children;
};

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

actor lookup_remote_named_actor(actor_system& sys, const std::string& host,
                                uint16_t port, const std::string& actor_name) {
  auto nid = with_retry([&] { return sys.middleman().connect(host, port); });
  if (!nid)
    return {};
  auto actor_ptr = with_retry(
    [&] { return sys.middleman().remote_lookup(actor_name, *nid); });
  if (!actor_ptr)
    return {};
  return actor_cast<actor>(actor_ptr);
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

std::optional<register_reply> request_register(scoped_actor& self,
                                               const actor& master,
                                               const node_manifest& manifest) {
  std::optional<register_reply> reply;
  self->request(master, 10s, master_register_atom_v, manifest).receive(
    [&](const register_reply& value) {
      reply = value;
    },
    [&](const error& err) {
      self->println("registration failed: {}", to_string(err));
    }
  );
  return reply;
}

std::optional<actor_route> request_route(scoped_actor& self, const actor& master,
                                         const std::string& node_name,
                                         const std::string& actor_name) {
  std::optional<actor_route> route;
  self->request(master, 10s, master_resolve_atom_v, node_name, actor_name)
    .receive(
      [&](const actor_route& value) {
        route = value;
      },
      [&](const error& err) {
        self->println("resolve {}:{} failed: {}", node_name, actor_name,
                      to_string(err));
      }
    );
  return route;
}

std::optional<topology_snapshot> request_topology(scoped_actor& self,
                                                  const actor& master) {
  std::optional<topology_snapshot> snapshot;
  self->request(master, 10s, master_topology_atom_v).receive(
    [&](const topology_snapshot& value) {
      snapshot = value;
    },
    [&](const error& err) {
      self->println("topology request failed: {}", to_string(err));
    }
  );
  return snapshot;
}

std::optional<child_snapshot> request_children(scoped_actor& self,
                                               const actor& master,
                                               const std::string& parent_name) {
  std::optional<child_snapshot> snapshot;
  self->request(master, 10s, master_children_atom_v, parent_name).receive(
    [&](const child_snapshot& value) {
      snapshot = value;
    },
    [&](const error& err) {
      self->println("children request failed: {}", to_string(err));
    }
  );
  return snapshot;
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

std::optional<node_manifest> first_child_of_kind(const child_snapshot& children,
                                                 node_kind kind) {
  for (const auto& child : children.children) {
    if (child.kind == kind)
      return child;
  }
  return std::nullopt;
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

bool register_with_master(actor_system& sys, const node_config& cfg,
                          const node_manifest& manifest, actor& master_actor) {
  master_actor = lookup_remote_named_actor(sys, cfg.master_host, cfg.master_port,
                                           k_master_control);
  if (!master_actor) {
    sys.println("[{}] failed to lookup master actor at {}:{}", cfg.name,
                cfg.master_host, cfg.master_port);
    return false;
  }
  scoped_actor self{sys};
  auto reply = request_register(self, master_actor, manifest);
  if (!reply || !reply->ok) {
    sys.println("[{}] registration was rejected", cfg.name);
    return false;
  }
  sys.println("[{}] registered with master: {}", cfg.name, reply->message);
  return true;
}

void attach_to_parent_region(actor_system& sys, const node_manifest& manifest,
                             const actor& master_actor) {
  if (manifest.parent.empty())
    return;
  scoped_actor self{sys};
  auto route = request_route(self, master_actor, manifest.parent, k_region_router);
  if (!route) {
    sys.println("[{}] could not resolve parent region '{}'", manifest.node_name,
                manifest.parent);
    return;
  }
  auto region_actor = lookup_remote_named_actor(sys, route->host, route->port,
                                                route->actor_name);
  if (!region_actor) {
    sys.println("[{}] could not lookup parent region actor '{}'",
                manifest.node_name, manifest.parent);
    return;
  }
  self->request(region_actor, 10s, region_attach_atom_v, manifest).receive(
    [&](const register_reply& reply) {
      sys.println("[{}] parent attach: {}", manifest.node_name, reply.message);
    },
    [&](const error& err) {
      sys.println("[{}] parent attach failed: {}", manifest.node_name,
                  to_string(err));
    }
  );
}
