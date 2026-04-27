#pragma once

#include <caf/actor_system_config.hpp>

struct config : caf::actor_system_config {
    std::string cmd_addr{"0.0.0.0"};
    uint16_t cmd_port{};

	uint16_t http_port{8080};
	std::string cert_file;
	std::string key_file;

    config();
};
