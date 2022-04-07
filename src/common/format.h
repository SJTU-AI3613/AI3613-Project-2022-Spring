#pragma once

#include <fmt/core.h>
#include <fmt/format.h>

namespace fmt {
struct naivedb_base_formatter {
    constexpr auto parse(format_parse_context &ctx) -> decltype(ctx.begin()) {
        auto it = ctx.begin(), end = ctx.end();
        if (it != end && *it != '}')
            throw format_error("invalid format");
        return it;
    }
};
}  // namespace fmt