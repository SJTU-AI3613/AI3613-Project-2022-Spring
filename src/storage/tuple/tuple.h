#pragma once

#include "common/format.h"
#include "common/macros.h"
#include "common/types.h"

#include <cstddef>
#include <vector>

namespace naivedb {
namespace type {
class Value;
}
namespace catalog {
class Schema;
}
}  // namespace naivedb

namespace naivedb::storage {
class Tuple {
    DISALLOW_COPY(Tuple)

  public:
    Tuple() = default;

    explicit Tuple(std::vector<char> &&data) : data_(std::move(data)) {}

    Tuple(const std::vector<type::Value> &values);

    Tuple(Tuple &&tuple) : data_(std::move(tuple.data_)) {}

    Tuple &operator=(Tuple &&tuple) {
        data_ = std::move(tuple.data_);
        return *this;
    }

    bool operator==(const Tuple &other) const { return data_ == other.data_; }

    bool operator!=(const Tuple &other) const { return data_ != other.data_; }

    size_t size() const { return data_.size(); }

    const std::vector<char> &data() const { return data_; }

    type::Value value_at(const catalog::Schema *schema, column_id_t column_id) const;

    void set_value_at(const catalog::Schema *schema, column_id_t column_id, const type::Value &value);

    std::vector<type::Value> values(const catalog::Schema *schema) const;

  private:
    std::vector<char> data_;
};
}  // namespace naivedb::storage

namespace fmt {
template <>
struct formatter<naivedb::storage::Tuple> : public naivedb_base_formatter {
    template <typename FormatContext>
    auto format(const naivedb::storage::Tuple &obj, FormatContext &ctx) -> decltype(ctx.out()) {
        return format_to(ctx.out(), "Tuple {{ data_: [{:#x}] }}", join(obj.data(), ", "));
    }
};
}  // namespace fmt