#include "database.h"

#include <sqlite3.h>

database::~database() {
    if (db_) {
        sqlite3_close(db_);
    }
}

caf::error database::open() {
    if (db_) {
        //return caf::make_error(ec::database_already_open);
    }
    int rc = sqlite3_open(db_file_.c_str(), &db_);
    if (rc != SQLITE_OK) {
        return caf::make_error(caf::sec::runtime_error, sqlite3_errmsg(db_));
    }

    char const* create_table_sql = R"sql(
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY,
            price INTEGER NOT NULL,
            available INTEGER NOT NULL,
            name TEXT NOT NULL
        );
    )sql";
    char* err_msg = nullptr;
    rc = sqlite3_exec(db_, create_table_sql, nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
        std::string error_message = err_msg ? err_msg : "unknown error";
        sqlite3_free(err_msg);
        return caf::make_error(caf::sec::runtime_error, std::move(error_message));
    }
    return {};
}

int database::count() {
    if (!db_) {
        return -1;
    }
    const char* count_sql = "SELECT COUNT(*) FROM items;";
    sqlite3_stmt* stmt{};
    int rc = sqlite3_prepare_v2(db_, count_sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return -1;
    }
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        return -1;
    }
    int count = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    return count;
}

std::optional<item> database::get(int32_t id) {
    if (!db_) {
        return std::nullopt;
    }
    const char* get_sql = "SELECT id, price, available, name FROM items WHERE id = ?;";
    sqlite3_stmt* stmt{};
    int rc = sqlite3_prepare_v2(db_, get_sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return std::nullopt;
    }
    sqlite3_bind_int(stmt, 1, id);
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        return std::nullopt;
    }

    item result{
        sqlite3_column_int(stmt, 0),
        sqlite3_column_int(stmt, 1),
        sqlite3_column_int(stmt, 2),
        reinterpret_cast<const char*>(sqlite3_column_text(stmt, 3))
    };
    sqlite3_finalize(stmt);
    return result;
}

ec database::insert(item const& new_item) {
    if (!db_) {
        //return ec::database_not_open;
    }
    const char* insert_sql = "INSERT INTO items (id, price, available, name) VALUES (?, ?, ?, ?);";
    sqlite3_stmt* stmt{};
    int rc = sqlite3_prepare_v2(db_, insert_sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return ec::database_inaccessible;
    }
    if(sqlite3_bind_int(stmt, 1, new_item.id) != SQLITE_OK ||
       sqlite3_bind_int(stmt, 2, new_item.price) != SQLITE_OK ||
       sqlite3_bind_int(stmt, 3, new_item.available) != SQLITE_OK ||
       sqlite3_bind_text(stmt, 4, new_item.name.c_str(), -1, SQLITE_TRANSIENT) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        return ec::database_inaccessible;
    }
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        return ec::database_inaccessible;
    }
    return ec::nil;
}

ec database::inc(int32_t id, int32_t amount) {
    if (!db_) {
        //return ec::database_not_open;
    }
    const char* inc_sql = "UPDATE items SET available = available + ? WHERE id = ?;";
    sqlite3_stmt* stmt{};
    int rc = sqlite3_prepare_v2(db_, inc_sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return ec::database_inaccessible;
    }
    if(sqlite3_bind_int(stmt, 1, amount) != SQLITE_OK ||
       sqlite3_bind_int(stmt, 2, id) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        return ec::database_inaccessible;
    }
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        return ec::no_such_item;
    }
    
    return ec::nil;
}

ec database::dec(int32_t id, int32_t amount) {
    if (!db_) {
        //return ec::database_not_open;
    }
    const char* dec_sql = "UPDATE items SET available = CASE WHEN available < ? THEN 0 ELSE available - ? END WHERE id = ?;";
    sqlite3_stmt* stmt{};
    int rc = sqlite3_prepare_v2(db_, dec_sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return ec::database_inaccessible;
    }
    if(sqlite3_bind_int(stmt, 1, amount) != SQLITE_OK ||
       sqlite3_bind_int(stmt, 2, amount) != SQLITE_OK ||
       sqlite3_bind_int(stmt, 3, id) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        return ec::database_inaccessible;
    }
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        return ec::no_such_item;
    }
    return ec::nil;
}

ec database::del(int32_t id) {
    if (!db_) {
        //return ec::database_not_open;
    }
    const char* del_sql = "DELETE FROM items WHERE id = ?;";
    sqlite3_stmt* stmt{};
    int rc = sqlite3_prepare_v2(db_, del_sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return ec::database_inaccessible;
    }
    if(sqlite3_bind_int(stmt, 1, id) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        return ec::database_inaccessible;
    }
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        return ec::no_such_item;
    }
    return ec::nil;
}
