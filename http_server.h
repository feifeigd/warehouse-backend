#pragma once

#include "database_actor.h" // For `database_actor` and `item_events`.

#include <caf/error.hpp>
#include <caf/json_writer.hpp>
#include <caf/net/http/responder.hpp>
#include <caf/typed_actor.hpp>

class http_server{
public:
    using responder = caf::net::http::responder;
    static constexpr std::string_view json_mime_type = "application/json";

    http_server(database_actor db_actor) : db_actor_{std::move(db_actor)}
    {
        writer_.skip_object_type_annotation(true);
    }

    void get(responder& res, int32_t key);
    void add(responder& res, int32_t key, std::string const& name, int32_t price);
    void add(responder& res, int32_t key);
    void inc(responder& res, int32_t key, int32_t amount);
    void dec(responder& res, int32_t key, int32_t amount);
    void del(responder& res, int32_t key);
private:
    void respond_with_item(responder::promise& prom, item const& item);

    template<class Responder>    
    void respond_with_error(Responder& prom, std::string_view code)const {
        using status = caf::net::http::status;
        std::string body = R"_({"code":})_";
        body += code;
        body +="\"}";
        prom.respond(status::internal_server_error, json_mime_type, body);
    }

    template<class Responder>
    void respond_with_error(Responder& prom, caf::error const& reason) {
        using namespace std::literals;
        if (caf::type_id_v<ec> == reason.category()) {
            auto code = to_string(static_cast<ec>(reason.code()));
            respond_with_error(prom, code);
            return;
        }
        if (caf::sec::request_timeout == reason) {
            respond_with_error(prom, "timeout"sv);
            return;
        }
        respond_with_error(prom, "internal_error"sv);
    }

    database_actor db_actor_;
    caf::json_writer writer_;
};
