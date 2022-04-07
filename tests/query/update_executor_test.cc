#include "buffer/buffer_manager.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/format.h"
#include "common/utils.h"
#include "io/disk_manager.h"
#include "query/execution/execution_engine.h"
#include "query/expr/binary_expr.h"
#include "query/expr/column_expr.h"
#include "query/expr/const_expr.h"
#include "query/physical_plan/physical_seq_scan.h"
#include "query/physical_plan/physical_update.h"
#include "query/query_test_utils.h"
#include "storage/tuple/tuple.h"
#include "test_utils.h"
#include "type/type.h"
#include "type/type_id.h"

#include <cstdlib>
#include <cstring>
#include <memory>

using namespace naivedb;

int main() {
    remove("test.db");
    io::DiskManager dm("test.db");
    buffer::BufferManager bm(16, &dm);
    catalog::Catalog catalog(&bm);

    auto table_info = build_test_table1(&bm, &catalog);
    auto table_schema = table_info.schema();
    auto table_id = table_info.table_id();

    std::unique_ptr<query::Expr> predicate;

    auto id_col_id = table_schema->column_id("id");
    auto &id_col = table_schema->column(id_col_id);
    auto name_col_id = table_schema->column_id("name");
    auto age_col_id = table_schema->column_id("age");

    auto id = std::make_unique<query::ColumnExpr>(id_col_id, id_col.type());
    auto const_2 = std::make_unique<query::ConstExpr>(type::Value(2));
    auto id_lt_1 = std::make_unique<query::BinaryExpr>(query::BinaryOperator::Lt, std::move(id), std::move(const_2));

    predicate = std::move(id_lt_1);

    // UPDATE Person SET age = 999, name = 'unknown' WHERE id < 2;
    auto update =
        std::make_unique<query::PhysicalUpdate>(make_vector(std::make_pair(age_col_id, type::Value(999)),
                                                            std::make_pair(name_col_id, type::Value(20, "unknown"))),
                                                table_id,
                                                std::move(predicate));

    // SELECT * FROM Person;
    auto seq_scan = std::make_unique<query::PhysicalSeqScan>(table_schema, table_id);

    query::ExecutionEngine engine(&bm, &catalog);
    auto result = engine.execute(update.get());

    TEST_ASSERT_EQ(result.size(), 0);

    result = engine.execute(seq_scan.get());

    auto expect = make_vector(make_vector(type::Value(0), type::Value(20, "unknown"), type::Value(999)),
                              make_vector(type::Value(1), type::Value(20, "unknown"), type::Value(999)),
                              make_vector(type::Value(2), type::Value(20, "Carol"), type::Value(19)),
                              make_vector(type::Value(3), type::Value(20, "Dave"), type::Value(20)));

    TEST_ASSERT_EQ(result.size(), expect.size());

    for (size_t i = 0; i < result.size(); ++i) {
        TEST_ASSERT_EQ(result[i], expect[i]);
        fmt::print("{}\n", fmt::join(result[i].values(table_schema), ", "));
    }

    return EXIT_SUCCESS;
}