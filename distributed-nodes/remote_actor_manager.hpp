#pragma once
#include <unordered_map>
#include <string>
#include <caf/all.hpp>
#include <functional>

// 通用远程 actor 管理器，支持缓存、monitor、失效自动清理
// Key: 用户自定义（如 node/actor_name），Value: caf::actor
// Down 回调可选
template <class SelfT>
class remote_actor_manager {
public:
  using key_type = std::string;
  using actor_type = caf::actor;
  using down_callback = std::function<void(const key_type&, const caf::down_msg&)>;

  remote_actor_manager(SelfT* self)
    : self_(self) {}

  ~remote_actor_manager() {
    clear();
  }

  // 添加并 monitor actor，key 可自定义
  void add(const key_type& key, const actor_type& act) {
    if (!act)
      return;
    auto addr = act.address();
    cache_[key] = act;
    addr_to_key_[addr] = key;
    if constexpr (requires(SelfT* s, const actor_type& a) { s->monitor(a); }) {
      self_->monitor(act);
    }
  }
  
  // actor失效时的回调
  // down_msg 处理，返回被移除的 key
  bool handle_down(const caf::down_msg& dm) {
    auto it = addr_to_key_.find(dm.source);
    if (it != addr_to_key_.end()) {
      erase(it->second);
      if (down_cb_)
        down_cb_(it->second, dm);
      return true;
    }
    return false;
  }

  // 查找缓存
  actor_type find(const key_type& key) const {
    auto it = cache_.find(key);
    return it == cache_.end() ? actor_type{} : it->second;
  }

  // 失效/移除

  void erase(const key_type& key) {
    auto it = cache_.find(key);
    if (it != cache_.end()) {
      if constexpr (requires(SelfT* s, const actor_type& a) { s->demonitor(a); }) {
        self_->demonitor(it->second);
      }
      addr_to_key_.erase(it->second.address());
      cache_.erase(it);
    }
  }

  // 设置 down 回调
  void set_down_callback(down_callback cb) {
    down_cb_ = std::move(cb);
  }

  // 清空所有缓存
  void clear() {
    if constexpr (requires(SelfT* s, const actor_type& a) { s->demonitor(a); }) {
      for (auto& kv : cache_) {
          self_->demonitor(kv.second);
      }
    }
    cache_.clear();
    addr_to_key_.clear();
  }

private:
  SelfT* self_;
  std::unordered_map<key_type, actor_type> cache_;
  std::unordered_map<caf::actor_addr, key_type> addr_to_key_;
  down_callback down_cb_;
};
