#pragma once

#include "node_types.hpp"

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
  uint32_t cluster_request_timeout_ms = 10000;
  uint32_t cluster_register_retry_ms = 30000;
  uint32_t cluster_retry_interval_ms = 500;
  uint32_t shutdown_request_timeout_ms = 30000;
  uint32_t rpc_resolve_timeout_ms = 10000;
  uint32_t rpc_request_timeout_ms = 10000;
  uint32_t rpc_invalidate_timeout_ms = 5000;
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
      .add(cluster_request_timeout_ms, "cluster-request-timeout-ms",
           "cluster management request timeout in milliseconds")
      .add(cluster_register_retry_ms, "cluster-register-retry-ms",
           "total retry time for master/parent registration in milliseconds")
      .add(cluster_retry_interval_ms, "cluster-retry-interval-ms",
           "retry interval for cluster registration in milliseconds")
      .add(shutdown_request_timeout_ms, "shutdown-request-timeout-ms",
           "node shutdown request timeout in milliseconds")
      .add(rpc_resolve_timeout_ms, "rpc-resolve-timeout-ms",
           "RPC actor resolve timeout in milliseconds")
      .add(rpc_request_timeout_ms, "rpc-request-timeout-ms",
           "RPC service request timeout in milliseconds")
      .add(rpc_invalidate_timeout_ms, "rpc-invalidate-timeout-ms",
           "RPC cache invalidation timeout in milliseconds")
      .add(shutdown_parent_on_exit, "shutdown-parent-on-exit",
           "request parent shutdown when this node exits")
      .add(shutdown_children_on_exit, "shutdown-children-on-exit",
           "request child shutdown when this node exits")
      .add(lifetime, "lifetime,l",
           "node lifetime in seconds, 0 waits for Ctrl+C");
  }
};