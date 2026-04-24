#include "database_actor.h"

#include <caf/actor_from_state.hpp>
#include <caf/flow/multicaster.hpp>
#include <caf/scheduled_actor/flow.hpp>

#include <cstdint>
#include <utility>
#include <unordered_map>

struct database_actor_state {
    database_actor::pointer self;
    database_ptr db;
    
    caf::flow::multicaster<item_event> mcast;

    database_actor_state(database_actor::pointer self_ptr, database_ptr db_ptr, item_events* events) 
    : self{ self_ptr }, db{ db_ptr }, mcast{self} {
		*events = mcast.as_observable().to_publisher();
    }

    database_actor::behavior_type make_behavior();
};

database_actor::behavior_type database_actor_state::make_behavior() {
    return {
        [this](get_atom, int32_t id) -> caf::result<item> {
            if (auto value = db->get(id)) {
                return {std::move(*value)};
            }
            return caf::make_error(ec::no_such_item);
        },
        [this](add_atom, int32_t id, int32_t price,
               std::string const& name) -> caf::result<void> {
            auto value = item{id, price, 0, name};
            if (auto ec = db->insert(value); ec != ec::nil) {
                return caf::make_error(ec);
            }
            mcast.push(std::make_shared<item>(std::move(value)));
            return {};
        },
        [this](inc_atom, int32_t id, int32_t amount) -> caf::result<int32_t> {
            if (auto ec = db->inc(id, amount); ec != ec::nil) {
                return caf::make_error(ec);
            }
            if (auto value = db->get(id)) {
                auto result = value->available;
                mcast.push(std::make_shared<item>(std::move(*value)));
                return result;
            }
            return caf::make_error(ec::no_such_item);
        },
        [this](dec_atom, int32_t id, int32_t amount) -> caf::result<int32_t> {
            if (auto ec = db->dec(id, amount); ec != ec::nil) {
                return caf::make_error(ec);
            }
            if (auto value = db->get(id)) {
                auto result = value->available;
                mcast.push(std::make_shared<item>(std::move(*value)));
                return result;
            }
            return caf::make_error(ec::no_such_item);
        },
        [this](del_atom, int32_t id) -> caf::result<void> {
            auto value = db->get(id);
            if (!value) {
                return caf::make_error(ec::no_such_item);
            }
             if (auto ec = db->del(id); ec != ec::nil) {
                return caf::make_error(ec);
            }
            value->available = 0;
            mcast.push(std::make_shared<item>(std::move(*value)));
            return caf::unit;
        },
    };
}

std::pair<database_actor, item_events> spawn_database_actor(caf::actor_system& sys, database_ptr db) {
    // Note: the actor uses a blocking API(SQLite3) and thus should run in its own thread.
    item_events events;
    auto handle = sys.spawn<caf::detached>(caf::actor_from_state<database_actor_state>, db, &events);
    return {handle, std::move(events)};
}
