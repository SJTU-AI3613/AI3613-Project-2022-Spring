#pragma once

#include "common/format.h"
#include "type/type_id.h"

#include <string>

namespace naivedb::type {
class Type {
  public:
    Type() = default;

    explicit Type(TypeId type_id) : type_id_(type_id) {}

    TypeId type_id() const { return type_id_; }

    size_t size() const {
        return std::visit([](auto type_id) { return type_id.size(); }, type_id_);
    }

    bool operator==(const Type &other) const { return type_id_ == other.type_id_; }

    bool operator!=(const Type &other) const { return type_id_ != other.type_id_; }

  private:
    TypeId type_id_;
};
}  // namespace naivedb::type

namespace fmt {
template <>
struct formatter<naivedb::type::Type> : public naivedb_base_formatter {
    template <typename FormatContext>
    auto format(const naivedb::type::Type &obj, FormatContext &ctx) -> decltype(ctx.out()) {
        return format_to(ctx.out(), "Type {{ type_id_: {} }}", obj.type_id());
    }
};
}  // namespace fmt