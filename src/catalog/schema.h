#pragma once

#include "catalog/column.h"
#include "common/constants.h"
#include "common/format.h"
#include "common/macros.h"
#include "common/types.h"
#include "type/type.h"

#include <vector>

namespace naivedb::catalog {
class Schema {
  public:
    explicit Schema(std::vector<Column> &&columns) : columns_(std::move(columns)), size_(0) {
        for (auto c : columns_) {
            column_offsets_.emplace_back(size_);
            size_ += c.size();
        }
    }

    bool operator==(const Schema &other) const { return columns_ == other.columns_; }

    bool operator!=(const Schema &other) const { return columns_ != other.columns_; }

    const std::vector<Column> &columns() const { return columns_; }

    const Column &column(column_id_t column_id) const { return columns_[column_id]; }

    const std::vector<uint32_t> &column_offsets() const { return column_offsets_; }

    uint32_t column_offset(column_id_t column_id) const { return column_offsets_[column_id]; }

    column_id_t column_id(std::string_view column_name) const;

    size_t size() const { return size_; }

  private:
    std::vector<Column> columns_;
    std::vector<uint32_t> column_offsets_;
    uint32_t size_;
};
}  // namespace naivedb::catalog

namespace fmt {
template <>
struct formatter<naivedb::catalog::Schema> : public naivedb_base_formatter {
    template <typename FormatContext>
    auto format(const naivedb::catalog::Schema &obj, FormatContext &ctx) -> decltype(ctx.out()) {
        return format_to(ctx.out(),
                         "Schema {{ columns_: [{}], column_offsets: [{}], size_: {} }}",
                         join(obj.columns(), ", "),
                         join(obj.column_offsets(), ", "),
                         obj.size());
    }
};
}  // namespace fmt