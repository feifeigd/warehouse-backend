#include "database_actor.h"

#include <caf/actor_from_state.hpp>
#include <caf/flow/multicaster.hpp>
#include <caf/scheduled_actor/flow.hpp>

#include <cstdint>
#include <utility>
#include <unordered_map>

struct database_actor_state {
    database_actor::pointer self;
    std::unordered_map<int32_t, item> items;
    caf::flow::multicaster<item_event> mcast;

    database_actor_state(database_actor::pointer self_ptr, item_events* events) : self{ self_ptr }, mcast{self} {
		*events = mcast.as_observable().to_publisher();
    }

    database_actor::behavior_type make_behavior();
};

database_actor::behavior_type database_actor_state::make_behavior() {
    return {
        [this](get_atom, int32_t id) -> caf::result<item> {
            if (auto it = items.find(id); it != items.end()) {
                return it->second;
            }
            return caf::make_error(ec::no_such_item);
        },
        [this](add_atom, int32_t id, int32_t price,
               std::string const& name) -> caf::result<void> {
            if (items.contains(id)) {
                return caf::make_error(ec::key_already_exists);
            }
            auto value = item{id, price, 0, name};
            items[id] = value;
            mcast.push(std::make_shared<item>(std::move(value)));
            return {};
        },
        [this](inc_atom, int32_t id, int32_t amount) -> caf::result<int32_t> {
            auto it = items.find(id);
            if (it == items.end()) {
                return caf::make_error(ec::no_such_item);
            }
            auto& entry = it->second;
            entry.available += amount;
            return entry.available;
        },
        [this](dec_atom, int32_t id, int32_t amount) -> caf::result<int32_t> {
            auto it = items.find(id);
            if (it == items.end()) {
                return caf::make_error(ec::no_such_item);
            }
            auto& entry = it->second;
            entry.available -= amount;
            return entry.available;
        },
        [this](del_atom, int32_t id) -> caf::result<void> {
            if (!items.erase(id)) {
                return caf::make_error(ec::no_such_item);
            }
            return {};
        },
    };
}

std::pair<database_actor, item_events> spawn_database_actor(caf::actor_system& sys){
    // Note: the actor uses a blocking API(SQLite3) and thus should run in its own thread.
    item_events events;
    auto handle = sys.spawn<caf::detached>(caf::actor_from_state<database_actor_state>, &events);
    return {handle, std::move(events)};
}
