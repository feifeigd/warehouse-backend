#include "applog.hpp"

#include "controller_actor.h"
#include "types.hpp"

#include <caf/actor_system.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/flow/observable_builder.hpp>
#include <caf/flow/byte.hpp>
#include <caf/flow/string.hpp>
#include <caf/json_reader.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/type_id.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

using namespace std::literals;

// {"type": "inc", "id": 1, "amount": 2}
struct command {
    std::string type; // Either "inc" or "dec"
    int32_t id{};
    int32_t amount{};

    bool valid()const noexcept {
        return (type == "inc" || type == "dec");
    }
};

template <class Inspector>
bool inspect(Inspector& f, command& x) {
    return f.object(x).fields(
        f.field("type", x.type),
        f.field("id", x.id),
        f.field("amount", x.amount)
    );
}

// --(spawn-controller-actor-impl-part1-begin)--
caf::actor spawn_controller_actor(caf::actor_system& sys, caf::actor db_actor, caf::net::acceptor_resource<std::byte> events){
    return sys.spawn([events = std::move(events), db_actor = std::move(db_actor)](caf::event_based_actor* self) mutable {
        // For each buffer pair, we create a new flow...
        events.observe_on(self).for_each([self, db_actor](auto ev) {
            info("controller added a new client");
            
            auto [pull, push] = ev.data();
            pull.observe_on(self)
                // .. that converts the lines to commands ...
                .transform(caf::flow::byte::split_as_utf8_at('\n'))
                // --(spawn-controller-actor-impl-part1-end)-- 
                // --(spawn-controller-actor-impl-part2-begin)--
                .map([](caf::cow_string const& line) -> std::shared_ptr<command> {
                    debug("controller received line: {}", line);
                    caf::json_reader reader;
                    if (!reader.load(line.str())) {
                        error("controller failed to parse line as JSON: {}", reader.get_error());
                        return {};
                    }
                    auto ptr = std::make_shared<command>();
                    if (!reader.apply(*ptr)) {
                        error("controller failed to apply JSON to command struct: {}", reader.get_error());
                        return {};
                    }
                    return ptr;
                })
                // --(spawn-controller-actor-impl-part2-end)--
                // --(spawn-controller-actor-impl-part3-begin)--
                .concat_map([self, db_actor](std::shared_ptr<command> cmd) {
                    if (!cmd || !cmd->valid()) {
                        // If the `map` step failed, inject an error message.
                        auto str = R"_({"error":"invalid command"})_"s;
                        return self->make_observable()
                            .just(caf::cow_string{ std::move(str) })
                            .as_observable();
                    }

                    // Send the command to the database actor and convert the result message into an observable.
                    caf::flow::observable<int32_t> result;
                    if ("inc" == cmd->type) {
                        result = self->mail(inc_atom_v, cmd->id, cmd->amount)
                            .request(db_actor, 1s)
                            .as_observable<int32_t>();
                    }
                    else {
                        result = self->mail(dec_atom_v, cmd->id, cmd->amount)
                            .request(db_actor, 1s)
                            .as_observable<int32_t>();
                    }
                    // On error, we return an error message to the client.
                    return result
                        .map([cmd](int32_t res) {
                            debug("controller received result for : {} -> {}", *cmd, res);
                            auto str = R"_({"result":)_"s + std::to_string(res) + "}";
                            return caf::cow_string{ std::move(str) };
                        })
                        .on_error_return([cmd](caf::error const& what) {
                            error("controller received an error for : {} -> {}", *cmd, what);
                            auto str = R"_({"error":")_"s + to_string(what) + R"_("})_";
                            auto res = caf::cow_string{ std::move(str) };
                            return caf::expected<caf::cow_string>{std::move(res)};
                        })
                        .as_observable();
                })
			    // --(spawn-controller-actor-impl-part3-end)--
			    // --(spawn-controller-actor-impl-part4-begin)--
                // ... disconnects if the client is too slow ...
                .on_backpressure_buffer(32)
                // ... and pushes the results back to the client as bytes.
                .transform(caf::flow::string::to_chars("\n"))
                .do_finally([]() {
				    info("controller lost connection to a client");
                })
                .map([](char ch) {
                    return static_cast<std::byte>(ch); 
                })
			    .subscribe(push);
        });
    });
}
