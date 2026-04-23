#pragma once

#include "item.hpp" // item
#include "types.hpp"    // For `inc_atom` and `dec_atom`.

#include <caf/typed_actor.hpp>

#include <cstdint>
#include <utility>

struct database_trait {
    using signatures = caf::type_list<
        // Retrieves an item from the database by its ID.
        caf::result<item>(get_atom, int32_t),
		caf::result<void>(add_atom, int32_t, int32_t, std::string),
        caf::result<int32_t>(inc_atom, int32_t, int32_t),
        caf::result<int32_t>(dec_atom, int32_t, int32_t),
        caf::result<void>(del_atom, int32_t)
    >;
};

using database_actor = caf::typed_actor<database_trait>;

std::pair<database_actor, item_events> spawn_database_actor(caf::actor_system& sys);
