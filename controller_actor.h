#pragma once

#include "database_actor.h" // For `database_actor` and `item_events`.

#include <caf/actor.hpp>
#include <caf/async/spsc_buffer.hpp>
#include <caf/net/acceptor_resource.hpp>
#include <caf/net/fwd.hpp>

#include <cstddef>

caf::actor spawn_controller_actor(caf::actor_system& sys, database_actor db_actor, caf::net::acceptor_resource<std::byte> events);
