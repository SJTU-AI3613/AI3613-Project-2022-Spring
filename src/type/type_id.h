#pragma once

#include "common/format.h"
#include "common/utils.h"

#include <cstdint>
#include <string>
#include <variant>

namespace naivedb::type {
class Boolean {
  public:
    Boolean() = default;

    size_t size() const { return sizeof(bool); }

    bool operator==(const Boolean &other) const { return true; }
    bool operator!=(const Boolean &other) const { return false; }
};

class Int {
  public:
    Int() = default;

    size_t size() const { return sizeof(int); }

    bool operator==(const Int &other) const { return true; }
    bool operator!=(const Int &other) const { return false; }
};

class Char {
  public:
    explicit Char(uint32_t len) : len_(len) {}

    // with header
    size_t size() const { return len_ + sizeof(uint32_t); }

    size_t len() const { return len_; }

    bool operator==(const Char &other) const { return len_ == other.len_; }
    bool operator!=(const Char &other) const { return len_ != other.len_; }

  private:
    uint32_t len_;
};

using TypeId = std::variant<Boolean, Int, Char>;
}  // namespace naivedb::type

namespace fmt {
template <>
struct formatter<naivedb::type::TypeId> : public naivedb_base_formatter {
    template <typename FormatContext>
    auto format(const naivedb::type::TypeId &obj, FormatContext &ctx) -> decltype(ctx.out()) {
        using namespace naivedb;
        using namespace type;
        return std::visit(overload{[&](Boolean) { return format_to(ctx.out(), "BOOLEAN"); },
                                   [&](Int) { return format_to(ctx.out(), "INT"); },
                                   [&](Char c) { return format_to(ctx.out(), "CHAR({})", c.len()); }},
                          obj);
    }
};
}  // namespace fmt