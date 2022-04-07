#pragma once

#include "common/format.h"
#include "type/type.h"

#include <string>
#include <string_view>

namespace naivedb::catalog {
class Column {
  public:
    explicit Column(const std::pair<std::string_view, type::Type> &pair)
        : column_name_(pair.first), type_(pair.second) {}
    Column(std::string_view column_name, type::Type type) : column_name_(column_name), type_(type) {}

    bool operator==(const Column &other) const { return column_name_ == other.column_name_ && type_ == other.type_; }

    bool operator!=(const Column &other) const { return column_name_ != other.column_name_ || type_ != other.type_; }

    std::string_view name() const { return column_name_; }

    type::Type type() const { return type_; }

    size_t size() const { return type_.size(); }

  private:
    std::string column_name_;
    type::Type type_;
};
}  // namespace naivedb::catalog

namespace fmt {
template <>
struct formatter<naivedb::catalog::Column> : public naivedb_base_formatter {
    template <typename FormatContext>
    auto format(const naivedb::catalog::Column &obj, FormatContext &ctx) -> decltype(ctx.out()) {
        return format_to(ctx.out(), "Column {{ column_name_: {}, type_: {} }}", obj.name(), obj.type());
    }
};
}  // namespace fmt