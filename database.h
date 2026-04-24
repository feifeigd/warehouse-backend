#pragma once

#include "ec.h"   // For `ec`.
#include "item.hpp" // For `item`.

#include <caf/error.hpp>

#include <memory>
#include <optional>
#include <string>

extern "C" {
    struct sqlite3;
}

/// SQLite3 的简单封装
class database {
    std::string db_file_;
    sqlite3* db_ = nullptr;
public:
    database(std::string const& db_file): db_file_(db_file) {

    }

    ~database();

    [[nodiscard]]
    caf::error open();

    [[nodiscard]]
    int count();

    [[nodiscard]]
    std::optional<item> get(int32_t id);

    [[nodiscard]]
    ec insert(item const& new_item);

    [[nodiscard]]
    ec inc(int32_t id, int32_t amount);

    [[nodiscard]]
    ec dec(int32_t id, int32_t amount);

    [[nodiscard]]
    ec del(int32_t id);
};

using database_ptr = std::shared_ptr<database>;
