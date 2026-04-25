
#include "applog.hpp"
#include "config.h"
#include "controller_actor.h"
#include "http_server.h"
#include "item.hpp"
#include "types.hpp"

#include <caf/caf_main.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/flow/observable_builder.hpp>
#include <caf/io/middleman.hpp>
#include <caf/net/http/with.hpp>
#include <caf/net/middleman.hpp>
#include <caf/net/octet_stream/with.hpp>
#include <caf/net/web_socket/frame.hpp>
#include <caf/net/web_socket/switch_protocol.hpp>
#include <caf/json_writer.hpp>

#include <chrono>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <string_view>
#include <thread>

using namespace std::literals;

namespace http = caf::net::http;
namespace ws   = caf::net::web_socket;


namespace {
	volatile std::sig_atomic_t shutdown_flag = 0;
	volatile std::sig_atomic_t shutdown_signal = 0;
	void set_shutdown_flag(int signal_number) {
		shutdown_flag = 1;
		shutdown_signal = signal_number;
	}

	constexpr auto default_max_connections		= 128;
	constexpr auto default_max_request_size		= 65'536; // 64 KiB
	constexpr auto default_max_pending_frames	= 32;
	constexpr std::string_view json_mime_type	= "application/json";
}

void ws_worker(caf::event_based_actor* self, caf::net::accept_event<ws::frame> new_conn, item_events events) {
	// 读/写
	auto [pull, push] = new_conn.data();
	// 忽略客户端输入
	pull.observe_on(self)
		.do_finally([]() {info("WebSocket client disconnected."); })
		.subscribe(std::ignore);

	auto writer = std::make_shared<caf::json_writer>();
	writer->skip_object_type_annotation(true);

	// 写
	events.observe_on(self)
		.filter([](item_event const& item) -> bool{return !!item; })
		.map([writer](item_event const& item) -> ws::frame {
			writer->reset();
			if (!writer->apply(*item)) {
				error("failed to serialize an item event: {}", writer->get_error());
				return {};
			}
			return ws::frame{writer->str()};
		})
		.filter([](ws::frame const& frame) {return !frame.empty(); })
		.on_backpressure_buffer(default_max_pending_frames)
		.subscribe(push);
}

int caf_main(caf::actor_system& sys, config const& cfg){
	// Do a regular shutdown when receiving SIGINT/CTRL+C or SIGTERM.
	signal(SIGINT,  set_shutdown_flag);
	signal(SIGTERM, set_shutdown_flag);

	auto current_working_directory = std::filesystem::current_path();
	info("Current working directory: {}", current_working_directory.string());

	auto db_file = current_working_directory / "warehouse.db";
	database_ptr db = std::make_shared<database>(db_file.string());
	if (auto err = db->open()) {
		error("Failed to open database: {}", err);
		return EXIT_FAILURE;
	}
	info("Database opened successfully, current item count: {}", db->count());

	auto [db_actor, events] = spawn_database_actor(sys, db);

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
	// --(http-server-config-begin)--
	namespace http = caf::net::http;
	namespace ssl = caf::net::ssl;
	auto cert_file = cfg.cert_file;
	auto key_file = cfg.key_file;
	if (!cert_file.empty() != !key_file.empty()) {
		error("*** inconsistent TLS config: declare neither file or both");
		return EXIT_FAILURE;
	}
	auto pem = ssl::format::pem;
	auto enable_tls = !cert_file.empty() && !key_file.empty();
	auto max_connections = caf::get_or(cfg, "max-connections", default_max_connections);
	auto max_request_size = caf::get_or(cfg, "max-request-size", default_max_request_size);

	// --(http-server-config-end)--
	// --(http-server-part1-begin)--
	auto impl = std::make_shared<http_server>(db_actor);

	auto server = http::with(sys)
		// Optionally enable TLS.
		.context(ssl::context::enable(enable_tls)
			.and_then(ssl::emplace_server(ssl::tls::v1_2))
			.and_then(ssl::use_private_key_file(key_file, pem))
			.and_then(ssl::use_certificate_file(cert_file, pem)))
		// Bind to the user-defined port.
		.accept(cfg.http_port)
		// Limit how many clients  may connected at any given time.
		.max_connections(max_connections)
		.max_request_size(max_request_size)
		// Stop the server if our database actor terminates.
		.monitor(db_actor)
		// --(http-server-part1-end)--
		// --(http-server-part2-begin)--
		// Minimal health endpoint.
		.route("/status", http::method::get,
			[](http::responder& res) {
				res.respond(http::status::ok, "text/plain", "ok");
			})
		// --(http-server-part2-end)--
		// --(http-server-part3-begin)--
		// WebSocket route for subscribing to item events.
		.route("/events", http::method::get,
			ws::switch_protocol()
				.on_request([](ws::acceptor<>& acceptor) {acceptor.accept(); })
				.on_start([&sys, ev = events](auto res) {
					// Spawn a server for the websocket connection that simply
					// spawns new workers for each incoming connection.
					sys.spawn([res = std::move(res), ev = std::move(ev)](caf::event_based_actor* self) {
						res.observe_on(self).for_each([self, ev = std::move(ev)](auto new_conn) {
							info("Websocket client connented");
							self->spawn(ws_worker, std::move(new_conn), ev);
						});
					});
				}))
		// 注册 http 路由
		.route("/item/<arg>", http::method::get, [impl](http::responder& res, int32_t key) {
			debug("GET /item/{}", key);
			impl->get(res, key);
		})
		.route("/item/<arg>", http::method::post, [impl](http::responder& res, int32_t key) {
			debug("POST /item/{}, body: {}", key, res.body());
			impl->add(res, key);
		})
		.route("/item/<arg>/inc/<arg>", http::method::put, [impl](http::responder& res, int32_t key, int32_t amount) {
			debug("PUT /item/{}/inc/{}", key, amount);
			impl->inc(res, key, amount);
		})
		.route("/item/<arg>/dec/<arg>", http::method::put, [impl](http::responder& res, int32_t key, int32_t amount) {
			debug("PUT /item/{}/dec/{}", key, amount);
			impl->dec(res, key, amount);
		})
		.route("/item/<arg>", http::method::del, [impl](http::responder& res, int32_t key) {
			debug("DELETE /item/{}/", key);
			impl->del(res, key);
		})
		// Start the server.
		.start();
		// --(http-server-part3-end)--

	if (!server) {
		error("Failed to start HTTP server: {}", server.error());
		return EXIT_FAILURE;
	}
	info("*** running at port {} with TLS {}abled, max connections: {}, max request size: {} bytes. Press CTRL+C to terminate the server.",
		cfg.http_port, enable_tls ? "en" : "dis", max_connections, max_request_size);


	while (!shutdown_flag) {
		std::this_thread::sleep_for(100ms);
	}

	if (shutdown_signal != 0) {
		warning("Received shutdown signal: {}", static_cast<int>(shutdown_signal));
	}

	warning("*** shutting down");
	//server->dispose();

	anon_send_exit(db_actor, caf::exit_reason::user_shutdown);

    return EXIT_SUCCESS;
}

CAF_MAIN(caf::net::middleman, caf::id_block::warehouse_backend)
CAF_MAIN(caf::io::middleman, caf::net::middleman, caf::id_block::warehouse_backend)
