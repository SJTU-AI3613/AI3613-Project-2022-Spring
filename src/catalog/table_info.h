#pragma once

#include "common/format.h"
#include "common/types.h"

#include <cassert>
#include <string_view>

namespace naivedb {
namespace catalog {
class Schema;
}
}  // namespace naivedb

namespace naivedb::catalog {
class TableInfo {
  public:
    TableInfo(table_id_t table_id, std::string_view table_name, const Schema *schema, page_id_t root_page_id)
        : table_id_(table_id), table_name_(table_name), schema_(schema), root_page_id_(root_page_id) {}

    table_id_t table_id() const { return table_id_; }

    std::string_view table_name() const { return table_name_; }

    const Schema *schema() const { return schema_; }

    page_id_t root_page_id() const { return root_page_id_; }

  private:
    table_id_t table_id_;
    std::string_view table_name_;
    const Schema *schema_;
    page_id_t root_page_id_;
};
}  // namespace naivedb::catalog

namespace fmt {
template <>
struct formatter<naivedb::catalog::TableInfo> : public naivedb_base_formatter {
    template <typename FormatContext>
    auto format(const naivedb::catalog::TableInfo &obj, FormatContext &ctx) -> decltype(ctx.out()) {
        assert(obj.schema());
        return format_to(ctx.out(),
                         "TableInfo {{ table_id_: {}, table_name_: {}, schema_: {}, root_page_id_: {} }}",
                         obj.table_id(),
                         obj.table_name(),
                         *obj.schema(),
                         obj.root_page_id());
    }
};
}  // namespace fmt