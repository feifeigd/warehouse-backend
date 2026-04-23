#include "database_actor.h"

#include <caf/actor_from_state.hpp>

struct database_actor_state {
    database_actor::pointer self;

    database_actor_state(database_actor::pointer self_ptr) : self{self_ptr} {

    }

    database_actor::behavior_type make_behavior();
};

database_actor::behavior_type database_actor_state::make_behavior() {
    return {
        [this](get_atom, int32_t id) -> item {
            return {};
        }
    };
}
std::pair<database_actor, item_events> spawn_database_actor(caf::actor_system& sys){
    // Note: the actor uses a blocking API(SQLite3) and thus should run in its own thread.
    item_events events;
    auto handle = sys.spawn<caf::detached>(caf::actor_from_state<database_actor_state>);
    return {handle, std::move(events)};
}
