#pragma once

#include <caf/default_enum_inspect.hpp>
#include <caf/is_error_code_enum.hpp>

#include <cstdint>
#include <string>
#include <string_view>

/// TODO 参考 CAF 怎么生成 error code enum，提供 to_string 和 from_string 的功能。

/// Application-specific error codes.
enum class ec : uint8_t {
    /// No error occurred.
    nil = 0,

    no_such_item,
    key_already_exists,
    database_inaccessible,
    invalid_argument,

    num_ec_codes,
};

std::string to_string(ec);

bool from_string(std::string_view, ec&);
bool from_integer(uint8_t, ec&);

template <class Inspector>
bool inspect(Inspector& f, ec& x){
    return caf::default_enum_inspect(f, x);
}

CAF_ERROR_CODE_ENUM(ec)
