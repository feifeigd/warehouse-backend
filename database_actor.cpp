#include "database_actor.h"

#include <caf/actor_from_state.hpp>

#include <cstdint>
#include <unordered_map>

struct database_actor_state {
    database_actor::pointer self;
    std::unordered_map<int32_t, item> items;

    database_actor_state(database_actor::pointer self_ptr) : self{self_ptr} {

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
        [this](inc_atom, int32_t id, int32_t amount) -> caf::result<int32_t> {
            auto& entry = items[id];
            entry.id = id;
            entry.available += amount;
            return entry.available;
        },
        [this](dec_atom, int32_t id, int32_t amount) -> caf::result<int32_t> {
            auto it = items.find(id);
            if (it == items.end()) {
                return caf::make_error(ec::no_such_item);
            }
            it->second.available -= amount;
            return it->second.available;
        },
    };
}

std::pair<database_actor, item_events> spawn_database_actor(caf::actor_system& sys){
    // Note: the actor uses a blocking API(SQLite3) and thus should run in its own thread.
    item_events events;
    auto handle = sys.spawn<caf::detached>(caf::actor_from_state<database_actor_state>);
    return {handle, std::move(events)};
}
