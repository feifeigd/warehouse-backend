#pragma once

#include <caf/actor_system_config.hpp>

struct config : caf::actor_system_config {
    std::string cmd_addr{"0.0.0.0"};
    uint16_t cmd_port{};

    config();
};
