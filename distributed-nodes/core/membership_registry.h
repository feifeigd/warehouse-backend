#pragma once

#include "app.hpp"

class monitored_node_registry {
public:
  struct slot {
    node_manifest manifest;
    actor monitor_actor;
    steady_clock_type::time_point expires_at = never_expires();
  };

  explicit monitored_node_registry(event_based_actor* self) : self_(self) {
    // nop
  }

  size_t size() const {
    return slots_.size();
  }

  bool contains(const std::string& node_name) const {
    return slots_.find(node_name) != slots_.end();
  }

  template <class ExpiryFn>
  void upsert(node_manifest manifest, actor monitor_actor, ExpiryFn expiry_for) {
    auto node_name = manifest.node_name;
    auto& slot = slots_[node_name];
    auto same_monitor = slot.monitor_actor && monitor_actor
                        && slot.monitor_actor.address() == monitor_actor.address();
    if (slot.monitor_actor && !same_monitor)
      self_->demonitor(slot.monitor_actor);
    slot.manifest = std::move(manifest);
    slot.monitor_actor = monitor_actor;
    slot.expires_at = expiry_for(slot.manifest);
    if (slot.monitor_actor && !same_monitor)
      self_->monitor(slot.monitor_actor);
  }

  template <class ExpiryFn>
  bool touch(const std::string& node_name, ExpiryFn expiry_for) {
    auto iter = slots_.find(node_name);
    if (iter == slots_.end())
      return false;
    iter->second.expires_at = expiry_for(iter->second.manifest);
    return true;
  }

  bool erase(const std::string& node_name, bool demonitor_actor) {
    auto iter = slots_.find(node_name);
    if (iter == slots_.end())
      return false;
    erase(iter, demonitor_actor);
    return true;
  }

  template <class LogFn>
  bool erase_by_monitor(const actor_addr& source, const error& reason,
                        LogFn on_erase) {
    for (auto iter = slots_.begin(); iter != slots_.end(); ++iter) {
      if (!iter->second.monitor_actor)
        continue;
      if (iter->second.monitor_actor.address() != source)
        continue;
      on_erase(iter->second.manifest, reason);
      erase(iter, false);
      return true;
    }
    return false;
  }

  template <class SkipFn, class LogFn>
  void prune_expired(steady_clock_type::time_point now, SkipFn skip,
                     LogFn on_erase) {
    std::vector<std::string> expired;
    for (const auto& [node_name, slot] : slots_) {
      if (skip(slot.manifest))
        continue;
      if (slot.expires_at <= now)
        expired.push_back(node_name);
    }
    for (const auto& node_name : expired) {
      auto iter = slots_.find(node_name);
      if (iter == slots_.end())
        continue;
      on_erase(iter->second.manifest);
      erase(iter, true);
    }
  }

  template <class Fn>
  void for_each_manifest(Fn fn) const {
    for (const auto& [_, slot] : slots_)
      fn(slot.manifest);
  }

  const node_manifest* find_manifest(const std::string& node_name) const {
    auto iter = slots_.find(node_name);
    if (iter == slots_.end())
      return nullptr;
    return &iter->second.manifest;
  }

private:
  using slots_map = std::unordered_map<std::string, slot>;

  void erase(slots_map::iterator iter, bool demonitor_actor) {
    if (demonitor_actor && iter->second.monitor_actor)
      self_->demonitor(iter->second.monitor_actor);
    slots_.erase(iter);
  }

  event_based_actor* self_;
  slots_map slots_;
};