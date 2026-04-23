
#include "applog.hpp"
#include "config.h"
#include "controller_actor.h"
#include "database_actor.h"
#include "item.hpp"
#include "types.hpp"

#include <caf/caf_main.hpp>
#include <caf/net/middleman.hpp>
#include <caf/net/octet_stream/with.hpp>

#include <atomic>

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

	auto [db_actor, events] = spawn_database_actor(sys);

	// --(ctrl-server-begin)--
	// Spin up the controller if configured.
	if(cfg.cmd_port) {
		auto cmd_server = caf::net::octet_stream::with(sys)
			// Bind to the user-defined port.
			.accept(cfg.cmd_port, cfg.cmd_addr)
			// Stop the server if our database actor terminates
			.monitor(db_actor)
			.start([&sys, db_actor](auto events) {
				// Log new connections and disconnections.
				info("cmd_server started, waiting for new connection..");
				auto cmd_server_processor = spawn_controller_actor(sys, db_actor, std::move(events));
			});
		if(!cmd_server) {
			error("Failed to start command server: {}", cmd_server.error());
			return EXIT_FAILURE;
		}
	}
	// --(ctrl-server-end)--


	while (!shutdown_flag) {
		std::this_thread::sleep_for(100ms);
	}

    return EXIT_SUCCESS;
}

CAF_MAIN(caf::net::middleman, caf::id_block::warehouse_backend)
