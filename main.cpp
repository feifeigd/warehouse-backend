
#include "applog.hpp"
#include "config.h"

#include <caf/caf_main.hpp>
#include <caf/net/middleman.hpp>
#include <caf/net/octet_stream/with.hpp>

#include <chrono>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <thread>

using namespace std::literals;

namespace {
	std::atomic<bool> shutdown_flag;
	void set_shutdown_flag(int) {
		shutdown_flag = true;
	}
}

int caf_main(caf::actor_system& sys, config const& cfg){
	// Do a regular shutdown when receiving SIGINT/CTRL+C or SIGTERM.
	signal(SIGINT,  set_shutdown_flag);
	signal(SIGTERM, set_shutdown_flag);

	auto current_working_directory = std::filesystem::current_path();
	info("Current working directory: {}", current_working_directory.string());

	// --(ctrl-server-begin)--
	// Spin up the controller if configured.
	if(cfg.cmd_port) {
		auto cmd_server = caf::net::octet_stream::with(sys)
			// Bind to the user-defined port.
			.accept(cfg.cmd_port, cfg.cmd_addr);
			
	}
	// --(ctrl-server-end)--


	while (!shutdown_flag) {
		std::this_thread::sleep_for(100ms);
	}

    return EXIT_SUCCESS;
}

CAF_MAIN(caf::net::middleman)
