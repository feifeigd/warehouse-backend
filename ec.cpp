#include "ec.h"

#include <array>
#include <cstddef>
#include <string>

namespace {

constexpr auto ec_names = std::array{
    "nil",
    "no_such_item",
    "key_already_exists",
    "database_inaccessible",
    "invalid_argument",
};

}

std::string to_string(ec code) {
    auto index = static_cast<size_t>(code);
    if (index < ec_names.size()) {
        return std::string{ec_names[index]};
    }
    return "-unknown-error-";
}

bool from_string(std::string_view str, ec& code) {
    for (size_t i = 0; i < ec_names.size(); ++i) {
        if (str == ec_names[i]) {
            code = static_cast<ec>(i);
            return true;
        }
    }
    return false;
}

bool from_integer(uint8_t val, ec& code) {
    if (val < ec_names.size()) {
        code = static_cast<ec>(val);
        return true;
    }
    return false;
}
