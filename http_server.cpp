#include "http_server.h"

#include <caf/net/actor_shell.hpp>
#include <caf/json_object.hpp>
#include <caf/json_value.hpp>

using namespace std::literals;
using http_status = caf::net::http::status;	// Ã¶¾Ù

void http_server::respond_with_item(responder::promise& prom, item const& value) {
	writer_.reset();
	if (!writer_.apply(value)) {
		respond_with_error(prom, "serialization_failed"sv);
		return;
	}
	prom.respond(http_status::ok, json_mime_type, writer_.str());
}


void http_server::get(responder& res, int32_t key) {
	auto self = res.self();	// actor_shell
	auto prom = std::move(res).to_promise();
	self->mail(get_atom_v, key)
		.request(db_actor_, 2s)
		.then([this, prom](item const& value)mutable {
			respond_with_item(prom, value);
		}, 
		[this, prom](caf::error const& what) mutable {
			if (ec::no_such_item == what) {
				respond_with_error(prom, to_string(what));
				return;
			}
			if (caf::sec::request_timeout == what) {
				respond_with_error(prom, "timeout");
				return;
			}
			respond_with_error(prom, "unexpected_database_result");
		});
}


void http_server::add(responder& res, int32_t key, std::string const& name, int32_t price) {
	auto self = res.self();
	auto prom = std::move(res).to_promise();
	self->mail(add_atom_v, key, price, name)
		.request(db_actor_, 2s)
		.then([prom]()mutable{
			prom.respond(http_status::created);
		}, [this, prom](caf::error const& what)mutable {
			respond_with_error(prom, what);
		});
}

void http_server::add(responder& res, int32_t key) {
	auto payload = res.payload();
	if (!caf::is_valid_utf8(payload)) {
		respond_with_error(res, "invalid_payload");
		return;
	}
	auto maybe_jval = caf::json_value::parse(caf::to_string_view(payload));
	if (!maybe_jval || !maybe_jval->is_object()) {
		respond_with_error(res, "invalid_payload");
		return;
	}

	auto obj = maybe_jval->to_object();
	auto price = obj.value("price");
	auto name = obj.value("name");
	return add(res, key, std::string{ name.to_string() }, static_cast<int32_t>(price.to_integer()));
}

void http_server::inc(responder& res, int32_t key, int32_t amount) {
	auto self = res.self();
	auto prom = std::move(res).to_promise();
	self->mail(inc_atom_v, key, amount)
		.request(db_actor_, 2s)
		.then([prom](int32_t res)mutable {
			prom.respond(http_status::no_content);
		}, [this, prom](caf::error const& what)mutable {
			respond_with_error(prom, what);
		});
}

void http_server::dec(responder& res, int32_t key, int32_t amount) {
	auto self = res.self();
	auto prom = std::move(res).to_promise();
	self->mail(dec_atom_v, key, amount)
		.request(db_actor_, 2s)
		.then([prom](int32_t res)mutable {
			prom.respond(http_status::no_content);
		}, [this, prom](caf::error const& what)mutable {
			respond_with_error(prom, what);
		});
}

void http_server::del(responder& res, int32_t key) {
	auto self = res.self();
	auto prom = std::move(res).to_promise();
	self->mail(del_atom_v, key)
		.request(db_actor_, 2s)
		.then([prom]()mutable {
			prom.respond(http_status::no_content);
		}, [this, prom](caf::error const& what) mutable {
			respond_with_error(prom, what);
		});
}
