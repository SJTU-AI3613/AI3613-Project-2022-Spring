#include "buffer/buffer_manager.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/utils.h"
#include "io/disk_manager.h"
#include "query/execution/execution_engine.h"
#include "query/expr/binary_expr.h"
#include "query/expr/column_expr.h"
#include "query/physical_plan/physical_hash_join.h"
#include "query/physical_plan/physical_seq_scan.h"
#include "query_test_utils.h"
#include "storage/tuple/tuple.h"
#include "test_utils.h"
#include "type/type.h"
#include "type/type_id.h"

#include <cstdlib>
#include <memory>

using namespace naivedb;

int main(int argc, char *argv[]) {
    remove("test.db");
    io::DiskManager dm("test.db");
    buffer::BufferManager bm(16, &dm);
    catalog::Catalog catalog(&bm);

    auto table_info1 = build_test_table1(&bm, &catalog);
    auto table_schema1 = table_info1.schema();
    auto table_id1 = table_info1.table_id();

    auto table_info2 = build_test_table2(&bm, &catalog);
    auto table_schema2 = table_info2.schema();
    auto table_id2 = table_info2.table_id();

    auto seq_scan1 = std::make_unique<query::PhysicalSeqScan>(table_schema1, table_id1);
    auto seq_scan2 = std::make_unique<query::PhysicalSeqScan>(table_schema2, table_id2);

    auto join_schema = catalog::Schema({
        {"id", type::Type(type::Int())},
        {"name", type::Type(type::Char(20))},
        {"age", type::Type(type::Int())},
        {"id", type::Type(type::Int())},
        {"salary", type::Type(type::Int())},
    });

    auto person_id_col_id = table_schema1->column_id("id");
    auto &person_id_col = table_schema1->column(person_id_col_id);
    auto person_id = std::make_unique<query::ColumnExpr>(person_id_col_id, person_id_col.type());

    auto salary_id_col_id = table_schema2->column_id("id");
    auto &salary_id_col = table_schema2->column(salary_id_col_id);
    auto salary_id = std::make_unique<query::ColumnExpr>(salary_id_col_id, salary_id_col.type(), false);

    // SELECT * FROM Person JOIN Salary ON Person.id = Salary.id;
    auto join = std::make_unique<query::PhysicalHashJoin>(
        &join_schema, std::move(seq_scan1), std::move(seq_scan2), std::move(person_id), std::move(salary_id));

    query::ExecutionEngine engine(&bm, &catalog);
    auto result = engine.execute(join.get());

    auto expect = make_vector(
        make_vector(type::Value(2), type::Value(20, "Carol"), type::Value(19), type::Value(2), type::Value(100)),
        make_vector(type::Value(3), type::Value(20, "Dave"), type::Value(20), type::Value(3), type::Value(200)));

    TEST_ASSERT_EQ(result.size(), expect.size());

    for (size_t i = 0; i < result.size(); ++i) {
        TEST_ASSERT_EQ(result[i], expect[i]);
        fmt::print("{}\n", fmt::join(result[i].values(&join_schema), ", "));
    }

    return EXIT_SUCCESS;
}