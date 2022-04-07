#include "buffer/buffer_manager.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/utils.h"
#include "io/disk_manager.h"
#include "query/execution/execution_engine.h"
#include "query/expr/column_expr.h"
#include "query/physical_plan/physical_projection.h"
#include "query/physical_plan/physical_seq_scan.h"
#include "query_test_utils.h"
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

    auto seq_scan = std::make_unique<query::PhysicalSeqScan>(table_schema, table_id);
    auto projection_schema = catalog::Schema({
        {"name", type::Type(type::Char(20))},
        {"id", type::Type(type::Int())},
    });
    auto name_col_id = table_schema->column_id("name");
    auto &name_col = table_schema->column(name_col_id);
    auto id_col_id = table_schema->column_id("id");
    auto &id_col = table_schema->column(id_col_id);

    std::vector<std::unique_ptr<const query::Expr>> project_exprs;
    project_exprs.emplace_back(std::make_unique<query::ColumnExpr>(name_col_id, name_col.type()));
    project_exprs.emplace_back(std::make_unique<query::ColumnExpr>(id_col_id, id_col.type()));

    // SELECT name, id FROM Person;
    auto projection =
        std::make_unique<query::PhysicalProjection>(&projection_schema, std::move(seq_scan), std::move(project_exprs));

    query::ExecutionEngine engine(&bm, &catalog);
    auto result = engine.execute(projection.get());

    auto expect = make_vector(make_vector(type::Value(20, "Alice"), type::Value(0)),
                              make_vector(type::Value(20, "Bob"), type::Value(1)),
                              make_vector(type::Value(20, "Carol"), type::Value(2)),
                              make_vector(type::Value(20, "Dave"), type::Value(3)));

    TEST_ASSERT_EQ(result.size(), expect.size());

    for (size_t i = 0; i < result.size(); ++i) {
        TEST_ASSERT_EQ(result[i], expect[i]);
        fmt::print("{}\n", fmt::join(result[i].values(&projection_schema), ", "));
    }

    return EXIT_SUCCESS;
}