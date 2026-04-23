#pragma once

#include "ec.h"
#include "item.hpp"

#include <caf/type_id.hpp>

CAF_BEGIN_TYPE_ID_BLOCK(warehouse_backend, first_custom_type_id)

    CAF_ADD_TYPE_ID(warehouse_backend, (ec))
    CAF_ADD_TYPE_ID(warehouse_backend, (item))

    // Used to retrieve an item from the database.
    CAF_ADD_ATOM(warehouse_backend, get_atom)

    // Used to add an item to the database.
    CAF_ADD_ATOM(warehouse_backend, add_atom)

    // Used to decrement an item's quantity in the database.
    CAF_ADD_ATOM(warehouse_backend, dec_atom)
    CAF_ADD_ATOM(warehouse_backend, inc_atom)
    CAF_ADD_ATOM(warehouse_backend, del_atom)
    
    // Used to signal a system shutdown to the control loop.
    CAF_ADD_ATOM(warehouse_backend, shutdown_atom)

CAF_END_TYPE_ID_BLOCK(warehouse_backend)
