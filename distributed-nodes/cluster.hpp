#pragma once

#include "node_config.hpp"

// cluster: main-thread synchronous client.
// Do not share across actors.
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

  bool has_master_actor() const {
    return static_cast<bool>(master_actor_);
  }

  void mark_master_unavailable() {
    master_actor_ = {};
  }

  std::chrono::milliseconds cluster_request_timeout() const {
    return std::chrono::milliseconds{cfg_.cluster_request_timeout_ms};
  }

  std::chrono::milliseconds cluster_register_retry() const {
    return std::chrono::milliseconds{cfg_.cluster_register_retry_ms};
  }

  std::chrono::milliseconds cluster_retry_interval() const {
    return std::chrono::milliseconds{cfg_.cluster_retry_interval_ms};
  }

  std::chrono::milliseconds shutdown_request_timeout() const {
    return std::chrono::milliseconds{cfg_.shutdown_request_timeout_ms};
  }
  
  bool register_with_master(const node_manifest& manifest,
                            const actor& monitor_actor) {
    auto registered = with_retry(
      [&]() -> std::optional<bool> {
        if (!master_actor_ && !connect_to_master())
          return {};
        scoped_actor self{sys_};
        auto reply = request_register(self,
                                      node_registration{manifest, monitor_actor});
        if (!reply || !reply->ok)
          return {};
        sys_.println("[{}] registered with master: {}", cfg_.name,
                     reply->message);
        return true;
      },
      cluster_register_retry(), cluster_retry_interval());
    if (!registered) {
      sys_.println("[{}] registration was rejected", cfg_.name);
      return false;
    }
    return true;
  }

  actor lookup_remote_named_actor(const std::string& host,
                                  uint16_t port, const std::string& actor_name) {
    return lookup_remote_actor(sys_, host, port, actor_name);
  }

  bool attach_to_parent_region(const node_manifest& manifest,
                               const actor& monitor_actor) {
    if (manifest.parent.empty())
      return true;

    auto attached = with_retry(
      [&]() -> std::optional<bool> {
        if (!master_actor_ && !connect_to_master())
          return {};
        scoped_actor self{sys_};
        auto route = request_route(self, manifest.parent, k_region_router);
        if (!route) {
          sys_.println("[{}] could not resolve parent region '{}'",
                       manifest.node_name, manifest.parent);
          return {};
        }
        auto region_actor = lookup_remote_named_actor(route->host, route->port,
                                                      route->actor_name);
        if (!region_actor) {
          sys_.println("[{}] could not lookup parent region actor '{}'",
                       manifest.node_name, manifest.parent);
          return {};
        }
        auto reply = request_actor<register_reply>(
          self, region_actor, cluster_request_timeout(), "parent attach",
          region_attach_atom_v, node_registration{manifest, monitor_actor});
        if (!reply)
          return {};
        sys_.println("[{}] parent attach: {}", manifest.node_name,
                     reply->message);
        return reply->ok ? std::optional<bool>{true} : std::nullopt;
      }
    , cluster_register_retry(), cluster_retry_interval());
    return static_cast<bool>(attached);
  }

  bool detach_from_parent_region(const node_manifest& manifest) {
    if (manifest.parent.empty())
      return true;
    if (!master_actor_)
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
    auto reply = request_actor<register_reply>(
      self, region_actor, cluster_request_timeout(), "parent detach",
      region_detach_atom_v, manifest.node_name);
    if (!reply)
      return false;
    sys_.println("[{}] parent detach: {}", manifest.node_name, reply->message);
    return reply->ok;
  }

  bool unregister_from_master(const std::string& node_name) {
    if (!master_actor_)
      return false;
    scoped_actor self{sys_};
    auto reply = request_master<register_reply>(
      self, cluster_request_timeout(), "unregister", master_unregister_atom_v,
      node_name);
    if (!reply)
      return false;
    sys_.println("[{}] unregister from master: {}", cfg_.name, reply->message);
    return reply->ok;
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
    auto reply = request_actor<register_reply>(
      self, control, shutdown_request_timeout(),
      "shutdown request to '" + node_name + "'",
      node_shutdown_atom_v, request
    );
    return reply && reply->ok;
  }

  std::optional<topology_snapshot> request_topology(scoped_actor& self) {
    return request_master<topology_snapshot>(
      self, cluster_request_timeout(), "topology request",
      master_topology_atom_v);
  }
  
  std::optional<actor_route> request_route(scoped_actor& self,
                                          const std::string& node_name,
                                          const std::string& actor_name) {
    if (!master_actor_)
      return {};
    std::optional<actor_route> route;
    self->request(master_actor_, cluster_request_timeout(),
                  master_resolve_atom_v, node_name, actor_name)
      .receive(
        [&](const actor_route& value) {
          route = value;
        },
        [&](const error& err) {
          sys_.println("[{}] resolve {}:{} failed: {}", cfg_.name, node_name, actor_name,
                        to_string(err));
          if (err != sec::no_such_key)
            mark_master_unavailable();
        }
      );
    return route;
  }
  
  std::optional<child_snapshot> request_children(scoped_actor& self,
                                                const std::string& parent_name) {
    return request_master<child_snapshot>(
      self, cluster_request_timeout(), "children request",
      master_children_atom_v, parent_name);
  }
private:

  // Helper function to perform retries with a timeout and interval.
  template <class Result, class... Args>
  std::optional<Result> request_actor(scoped_actor& self, const actor& target,
                                      std::chrono::milliseconds timeout,
                                      const std::string& context,
                                      Args&&... args) {
    if (!target)
      return {};
    std::optional<Result> result;
    self->request(target, timeout, std::forward<Args>(args)...).receive(
      [&](const Result& value) {
        result = value;
      },
      [&](const error& err) {
        sys_.println("[{}] {} failed: {}", cfg_.name, context, to_string(err));
      }
    );
    return result;
  }

  template <class Result, class... Args>
  std::optional<Result> request_master(scoped_actor& self,
                                       std::chrono::milliseconds timeout,
                                       const std::string& context,
                                       Args&&... args) {
    auto result = request_actor<Result>(self, master_actor_, timeout, context,
                                        std::forward<Args>(args)...);
    if (!result)
      mark_master_unavailable();
    return result;
  }

  std::optional<register_reply> request_register(scoped_actor& self,
                                                const node_registration& registration) {
    return request_master<register_reply>(
      self, cluster_request_timeout(), "registration", master_register_atom_v,
      registration);
  }
};
