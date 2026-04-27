#include <caf/actor_from_state.hpp>
#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/caf_main.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/io/middleman.hpp>
#include <caf/response_promise.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/type_id.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <deque>
#include <iostream>
#include <numeric>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace caf;
using namespace std::chrono_literals;

struct worker_profile {
  std::string name;
  std::string zone;
  uint32_t capacity = 1;
};

template <class Inspector>
bool inspect(Inspector& f, worker_profile& x) {
  return f.object(x).fields(
    f.field("name", x.name),
    f.field("zone", x.zone),
    f.field("capacity", x.capacity)
  );
}

struct cluster_ack {
  bool ok = false;
  std::string message;
};

template <class Inspector>
bool inspect(Inspector& f, cluster_ack& x) {
  return f.object(x).fields(
    f.field("ok", x.ok),
    f.field("message", x.message)
  );
}

struct job_request {
  uint64_t id = 0;
  std::string operation;
  std::vector<int32_t> lhs;
  std::vector<int32_t> rhs;
};

template <class Inspector>
bool inspect(Inspector& f, job_request& x) {
  return f.object(x).fields(
    f.field("id", x.id),
    f.field("operation", x.operation),
    f.field("lhs", x.lhs),
    f.field("rhs", x.rhs)
  );
}

struct job_result {
  uint64_t id = 0;
  std::string worker;
  bool ok = false;
  int64_t value = 0;
  std::string detail;
};

template <class Inspector>
bool inspect(Inspector& f, job_result& x) {
  return f.object(x).fields(
    f.field("id", x.id),
    f.field("worker", x.worker),
    f.field("ok", x.ok),
    f.field("value", x.value),
    f.field("detail", x.detail)
  );
}

struct cluster_snapshot {
  uint32_t workers = 0;
  uint64_t queued = 0;
  uint64_t dispatched = 0;
  uint64_t completed = 0;
  uint64_t failed = 0;
  std::vector<std::string> members;
};

template <class Inspector>
bool inspect(Inspector& f, cluster_snapshot& x) {
  return f.object(x).fields(
    f.field("workers", x.workers),
    f.field("queued", x.queued),
    f.field("dispatched", x.dispatched),
    f.field("completed", x.completed),
    f.field("failed", x.failed),
    f.field("members", x.members)
  );
}

CAF_BEGIN_TYPE_ID_BLOCK(distributed_actors_codex, first_custom_type_id)

  CAF_ADD_TYPE_ID(distributed_actors_codex, (worker_profile))
  CAF_ADD_TYPE_ID(distributed_actors_codex, (cluster_ack))
  CAF_ADD_TYPE_ID(distributed_actors_codex, (job_request))
  CAF_ADD_TYPE_ID(distributed_actors_codex, (job_result))
  CAF_ADD_TYPE_ID(distributed_actors_codex, (cluster_snapshot))

  CAF_ADD_ATOM(distributed_actors_codex, worker_join_atom)
  CAF_ADD_ATOM(distributed_actors_codex, job_submit_atom)
  CAF_ADD_ATOM(distributed_actors_codex, cluster_stats_atom)
  CAF_ADD_ATOM(distributed_actors_codex, run_job_atom)

CAF_END_TYPE_ID_BLOCK(distributed_actors_codex)

std::string join_names(const std::vector<std::string>& names) {
  if (names.empty())
    return "<none>";
  auto result = names.front();
  for (size_t index = 1; index < names.size(); ++index) {
    result += ", ";
    result += names[index];
  }
  return result;
}

job_result execute_job(std::string worker_name, const job_request& job) {
  job_result result;
  result.id = job.id;
  result.worker = std::move(worker_name);
  result.ok = true;
  if (job.operation == "sum") {
    result.value = std::accumulate(job.lhs.begin(), job.lhs.end(), int64_t{0});
    result.detail = "sum";
    return result;
  }
  if (job.operation == "max") {
    if (job.lhs.empty()) {
      result.ok = false;
      result.detail = "max requires at least one value";
      return result;
    }
    auto max_value = *std::max_element(job.lhs.begin(), job.lhs.end());
    result.value = max_value;
    result.detail = "max";
    return result;
  }
  if (job.operation == "dot") {
    if (job.lhs.size() != job.rhs.size()) {
      result.ok = false;
      result.detail = "dot requires vectors with the same length";
      return result;
    }
    int64_t value = 0;
    for (size_t index = 0; index < job.lhs.size(); ++index)
      value += static_cast<int64_t>(job.lhs[index]) * job.rhs[index];
    result.value = value;
    result.detail = "dot";
    return result;
  }
  result.ok = false;
  result.detail = "unknown operation: " + job.operation;
  return result;
}

behavior worker_actor_fun(event_based_actor* self, worker_profile profile) {
  return {
    [self, profile = std::move(profile)](run_job_atom, const job_request& job) {
      self->println("[worker:{}] accepted job {} ({})", profile.name, job.id,
                    job.operation);
      return execute_job(profile.name, job);
    },
  };
}

struct coordinator_state {
  struct queued_job {
    job_request job;
    response_promise promise;
  };

  struct worker_slot {
    actor handle;
    actor_addr addr;
    worker_profile profile;
    bool busy = false;
    std::optional<queued_job> inflight;
  };

  explicit coordinator_state(event_based_actor* selfptr) : self(selfptr) {
    // nop
  }

  behavior make_behavior() {
    return {
      [this](worker_join_atom, const actor& worker, worker_profile profile) {
        return register_worker(worker, std::move(profile));
      },
      [this](job_submit_atom, job_request job) -> result<job_result> {
        auto rp = self->make_response_promise();
        auto job_id = job.id;
        pending_jobs.push_back(queued_job{std::move(job), rp});
        self->println("[seed] queued job {}, waiting jobs={}", job_id,
                      pending_jobs.size());
        flush();
        return rp;
      },
      [this](cluster_stats_atom) {
        return snapshot();
      },
    };
  }

  cluster_ack register_worker(const actor& worker, worker_profile profile) {
    if (!worker)
      return {false, "worker registration failed: invalid actor handle"};
    auto addr = worker.address();
    if (auto* existing = find_worker(addr)) {
      existing->profile = std::move(profile);
      return {true, "worker metadata refreshed"};
    }
    auto label = profile.name;
    self->monitor(worker, [this, addr, label](const error& reason) {
      on_worker_down(addr, label, reason);
    });
    workers.push_back(worker_slot{worker, addr, std::move(profile)});
    self->println("[seed] worker '{}' joined, workers={}", label,
                  workers.size());
    flush();
    return {true, "worker registered"};
  }

  cluster_snapshot snapshot() const {
    cluster_snapshot value;
    value.workers = static_cast<uint32_t>(workers.size());
    value.queued = static_cast<uint64_t>(pending_jobs.size());
    value.dispatched = dispatched_jobs;
    value.completed = completed_jobs;
    value.failed = failed_jobs;
    value.members.reserve(workers.size());
    for (const auto& slot : workers)
      value.members.push_back(slot.profile.name);
    return value;
  }

  worker_slot* find_worker(const actor_addr& addr) {
    auto iter = std::find_if(workers.begin(), workers.end(),
                             [&](const worker_slot& slot) {
                               return slot.addr == addr;
                             });
    if (iter == workers.end())
      return nullptr;
    return &*iter;
  }

  worker_slot* next_idle_worker() {
    if (workers.empty())
      return nullptr;
    for (size_t offset = 0; offset < workers.size(); ++offset) {
      auto index = (round_robin_offset + offset) % workers.size();
      if (!workers[index].busy) {
        round_robin_offset = (index + 1) % workers.size();
        return &workers[index];
      }
    }
    return nullptr;
  }

  std::optional<queued_job> take_inflight(const actor_addr& addr) {
    auto* slot = find_worker(addr);
    if (slot == nullptr || !slot->inflight)
      return std::nullopt;
    auto value = std::move(slot->inflight);
    slot->inflight.reset();
    slot->busy = false;
    return value;
  }

  void dispatch(worker_slot& slot, queued_job job) {
    auto addr = slot.addr;
    auto worker_name = slot.profile.name;
    auto payload = job.job;
    auto job_id = payload.id;
    slot.busy = true;
    slot.inflight = std::move(job);
    ++dispatched_jobs;
    self->println("[seed] dispatch job {} -> {}", job_id, worker_name);
    self->request(slot.handle, worker_timeout, run_job_atom_v, payload)
      .then(
        [this, addr, worker_name](job_result result) {
          auto request = take_inflight(addr);
          if (!request)
            return;
          if (result.worker.empty())
            result.worker = worker_name;
          ++completed_jobs;
          self->println("[seed] completed job {} <- {}", result.id,
                        result.worker);
          request->promise.deliver(std::move(result));
          flush();
        },
        [this, addr, worker_name](const error& err) {
          auto request = take_inflight(addr);
          if (!request)
            return;
          ++failed_jobs;
          self->println("[seed] failed job {} on {}: {}", request->job.id,
                        worker_name, to_string(err));
          request->promise.deliver(
            job_result{request->job.id, worker_name, false, 0, to_string(err)});
          flush();
        }
      );
  }

  void flush() {
    while (!pending_jobs.empty()) {
      auto* slot = next_idle_worker();
      if (slot == nullptr)
        return;
      auto next = std::move(pending_jobs.front());
      pending_jobs.pop_front();
      dispatch(*slot, std::move(next));
    }
  }

  void on_worker_down(const actor_addr& addr, const std::string& label,
                      const error& reason) {
    auto iter = std::find_if(workers.begin(), workers.end(),
                             [&](const worker_slot& slot) {
                               return slot.addr == addr;
                             });
    if (iter == workers.end())
      return;
    if (iter->inflight) {
      self->println("[seed] worker '{}' left during job {}, requeueing", label,
                    iter->inflight->job.id);
      pending_jobs.push_front(std::move(*iter->inflight));
      iter->inflight.reset();
    }
    self->println("[seed] worker '{}' left: {}", label, to_string(reason));
    workers.erase(iter);
    if (workers.empty())
      round_robin_offset = 0;
    else if (round_robin_offset >= workers.size())
      round_robin_offset %= workers.size();
    flush();
  }

  static constexpr auto worker_timeout = 15s;

  event_based_actor* self;
  std::vector<worker_slot> workers;
  std::deque<queued_job> pending_jobs;
  uint64_t dispatched_jobs = 0;
  uint64_t completed_jobs = 0;
  uint64_t failed_jobs = 0;
  size_t round_robin_offset = 0;
};

job_request make_demo_job(uint64_t id) {
  switch (id % 3) {
    case 1:
      return job_request{
        id,
        "sum",
        {static_cast<int32_t>(id), static_cast<int32_t>(id + 1),
         static_cast<int32_t>(id + 2), static_cast<int32_t>(id + 3)},
        {}
      };
    case 2:
      return job_request{
        id,
        "max",
        {static_cast<int32_t>(id * 2), static_cast<int32_t>(id + 11),
         static_cast<int32_t>(id + 3), static_cast<int32_t>(id + 7)},
        {}
      };
    default:
      return job_request{
        id,
        "dot",
        {1, 2, 3, 4},
        {static_cast<int32_t>(id), 1, 2, 3}
      };
  }
}

struct config : actor_system_config {
  std::string mode = "seed";
  std::string host = "127.0.0.1";
  std::string bind;
  uint16_t port = 45500;
  std::string name = "worker-1";
  std::string zone = "default";
  uint32_t jobs = 6;
  uint32_t lifetime = 0;

  config() {
    opt_group{custom_options_, "global"}
      .add(mode, "mode,m", "seed | worker | client")
      .add(host, "host,H", "seed host for worker/client mode")
      .add(bind, "bind,B", "bind address for seed mode, empty means any")
      .add(port, "port,p", "TCP port for the published coordinator actor")
      .add(name, "name,n", "worker node name")
      .add(zone, "zone,z", "worker zone label")
      .add(jobs, "jobs,j", "number of demo jobs in client mode")
      .add(lifetime, "lifetime,l",
           "seed/worker lifetime in seconds, 0 waits for <enter>");
  }
};

void print_snapshot(actor_system& sys, const cluster_snapshot& snapshot) {
  sys.println("[client] cluster workers={}, queued={}, dispatched={}, "
              "completed={}, failed={}",
              snapshot.workers, snapshot.queued, snapshot.dispatched,
              snapshot.completed, snapshot.failed);
  sys.println("[client] members: {}", join_names(snapshot.members));
}

void run_seed(actor_system& sys, const config& cfg) {
  auto coordinator = sys.spawn(actor_from_state<coordinator_state>);
  const char* bind_addr = cfg.bind.empty() ? nullptr : cfg.bind.c_str();
  auto port = sys.middleman().publish(coordinator, cfg.port, bind_addr, true);
  if (!port) {
    sys.println("[seed] failed to publish coordinator: {}", port.error());
    anon_send_exit(coordinator, exit_reason::user_shutdown);
    return;
  }
  auto listen_addr = cfg.bind.empty() ? std::string{"0.0.0.0"} : cfg.bind;
  sys.println("[seed] coordinator published on {}:{}", listen_addr, *port);
  if (cfg.lifetime > 0) {
    sys.println("[seed] running for {} seconds", cfg.lifetime);
    std::this_thread::sleep_for(std::chrono::seconds{cfg.lifetime});
  } else {
    sys.println("[seed] press <enter> to shut down");
    std::string dummy;
    std::getline(std::cin, dummy);
  }
  anon_send_exit(coordinator, exit_reason::user_shutdown);
}

void run_worker(actor_system& sys, const config& cfg) {
  auto coordinator = sys.middleman().remote_actor(cfg.host, cfg.port);
  if (!coordinator) {
    sys.println("[worker] failed to connect to {}:{}: {}", cfg.host, cfg.port,
                coordinator.error());
    return;
  }
  auto profile = worker_profile{cfg.name, cfg.zone, 1};
  auto worker = sys.spawn(worker_actor_fun, profile);
  scoped_actor self{sys};
  auto registered = false;
  self->request(*coordinator, 10s, worker_join_atom_v, worker, profile).receive(
    [&](const cluster_ack& ack) {
      registered = ack.ok;
      self->println("[worker:{}] {}", profile.name, ack.message);
    },
    [&](const error& err) {
      self->println("[worker:{}] registration failed: {}", profile.name,
                    to_string(err));
    }
  );
  if (!registered) {
    anon_send_exit(worker, exit_reason::user_shutdown);
    return;
  }
  if (cfg.lifetime > 0) {
    self->println("[worker:{}] connected to {}:{}, running for {} seconds",
                  profile.name, cfg.host, cfg.port, cfg.lifetime);
    std::this_thread::sleep_for(std::chrono::seconds{cfg.lifetime});
  } else {
    self->println("[worker:{}] connected to {}:{}, press <enter> to stop",
                  profile.name, cfg.host, cfg.port);
    std::string dummy;
    std::getline(std::cin, dummy);
  }
  anon_send_exit(worker, exit_reason::user_shutdown);
}

void run_client(actor_system& sys, const config& cfg) {
  auto coordinator = sys.middleman().remote_actor(cfg.host, cfg.port);
  if (!coordinator) {
    sys.println("[client] failed to connect to {}:{}: {}", cfg.host, cfg.port,
                coordinator.error());
    return;
  }
  scoped_actor self{sys};
  for (uint64_t id = 1; id <= cfg.jobs; ++id) {
    auto job = make_demo_job(id);
    self->request(*coordinator, 30s, job_submit_atom_v, std::move(job)).receive(
      [&](const job_result& result) {
        if (result.ok) {
          self->println("[client] job {} handled by {} => {} ({})", result.id,
                        result.worker, result.value, result.detail);
        } else {
          self->println("[client] job {} failed on {}: {}", result.id,
                        result.worker, result.detail);
        }
      },
      [&](const error& err) {
        self->println("[client] request failed: {}", to_string(err));
      }
    );
  }
  self->request(*coordinator, 5s, cluster_stats_atom_v).receive(
    [&](const cluster_snapshot& snapshot) {
      print_snapshot(sys, snapshot);
    },
    [&](const error& err) {
      self->println("[client] stats request failed: {}", to_string(err));
    }
  );
}

void caf_main(actor_system& sys, const config& cfg) {
  if (cfg.mode == "seed")
    return run_seed(sys, cfg);
  if (cfg.mode == "worker")
    return run_worker(sys, cfg);
  if (cfg.mode == "client")
    return run_client(sys, cfg);
  sys.println("Unknown mode '{}'. Use seed, worker, or client.", cfg.mode);
}

CAF_MAIN(id_block::distributed_actors_codex, io::middleman)
