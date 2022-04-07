#include "buffer/buffer_manager.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/utils.h"
#include "io/disk_manager.h"
#include "query/execution/execution_engine.h"
#include "query/expr/binary_expr.h"
#include "query/expr/column_expr.h"
#include "query/expr/const_expr.h"
#include "query/physical_plan/physical_filter.h"
#include "query/physical_plan/physical_seq_scan.h"
#include "query/query_test_utils.h"
#include "storage/tuple/tuple.h"
#include "test_utils.h"
#include "type/type.h"
#include "type/type_id.h"

#include <cstdlib>
#include <cstring>
#include <memory>

using namespace naivedb;

int main(int argc, char *argv[]) {
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
    auto age_col_id = table_schema->column_id("age");
    auto &age_col = table_schema->column(age_col_id);

    auto id = std::make_unique<query::ColumnExpr>(id_col_id, id_col.type());
    auto const_1 = std::make_unique<query::ConstExpr>(type::Value(1));
    auto id_ge_1 = std::make_unique<query::BinaryExpr>(query::BinaryOperator::Ge, std::move(id), std::move(const_1));

    auto age = std::make_unique<query::ColumnExpr>(age_col_id, age_col.type());
    auto const_19 = std::make_unique<query::ConstExpr>(type::Value(19));
    auto age_ne_19 =
        std::make_unique<query::BinaryExpr>(query::BinaryOperator::Ne, std::move(age), std::move(const_19));

    auto id_ge_1_and_age_ne_19 =
        std::make_unique<query::BinaryExpr>(query::BinaryOperator::And, std::move(id_ge_1), std::move(age_ne_19));

    predicate = std::move(id_ge_1_and_age_ne_19);

    auto seq_scan = std::make_unique<query::PhysicalSeqScan>(table_schema, table_id);

    // SELECT * FROM Person WHERE id >= 1 AND age != 19;
    auto filter = std::make_unique<query::PhysicalFilter>(table_schema, std::move(seq_scan), std::move(predicate));

    query::ExecutionEngine engine(&bm, &catalog);
    auto result = engine.execute(filter.get());

    auto expect = make_vector(make_vector(type::Value(1), type::Value(20, "Bob"), type::Value(18)),
                              make_vector(type::Value(3), type::Value(20, "Dave"), type::Value(20)));

    TEST_ASSERT_EQ(result.size(), expect.size());

    for (size_t i = 0; i < result.size(); ++i) {
        TEST_ASSERT_EQ(result[i], expect[i]);
        fmt::print("{}\n", fmt::join(result[i].values(table_schema), ", "));
    }

    return EXIT_SUCCESS;
}