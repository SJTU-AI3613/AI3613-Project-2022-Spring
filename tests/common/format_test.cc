#include "catalog/column.h"
#include "catalog/schema.h"
#include "catalog/table_info.h"
#include "common/format.h"
#include "storage/tuple/tuple.h"
#include "test_utils.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value.h"

#include <cstdlib>

using namespace naivedb;

int main() {
    auto type_id = type::TypeId(type::Boolean());
    fmt::print("{}\n", type_id);

    type_id = type::TypeId(type::Int());
    fmt::print("{}\n", type_id);

    type_id = type::TypeId(type::Char(123));
    fmt::print("{}\n", type_id);

    auto type = type::Type(type_id);
    fmt::print("{}\n", type);

    auto value = type::Value(123);
    fmt::print("{}\n", value);

    value = type::Value(true);
    fmt::print("{}\n", value);

    value = type::Value(5, "abc");
    fmt::print("{}\n", value);

    value = type::Value(5, "abcdefg");
    fmt::print("{}\n", value);

    storage::Tuple tuple({1, 2, 3, 4, 5});
    fmt::print("{}\n", tuple);

    catalog::Column column("column_1", type);
    fmt::print("{}\n", column);

    catalog::Schema schema({
        {"column_1", type},
        {"column_2", type},
    });
    fmt::print("{}\n", schema);

    catalog::TableInfo table_info(123, "table", &schema, 1234);
    fmt::print("{}\n", table_info);

    return EXIT_SUCCESS;
}