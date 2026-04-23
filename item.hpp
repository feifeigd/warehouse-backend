#pragma once

#include <caf/async/publisher.hpp>

#include <memory>
#include <string>

struct item {
    int32_t id{};
    int32_t price{};
    int32_t available{};
    std::string name;
};

template <class Inspector>
bool inspect(Inspector& f, item& x) {
    return f.object(x).fields(
        f.field("id", x.id),
        f.field("price", x.price),
        f.field("available", x.available),
        f.field("name", x.name)
    );
}

using item_event = std::shared_ptr< const item>;
using item_events = caf::async::publisher<item_event>;
