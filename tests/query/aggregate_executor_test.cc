#include "buffer/buffer_manager.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/utils.h"
#include "io/disk_manager.h"
#include "query/execution/execution_engine.h"
#include "query/expr/aggregate_expr.h"
#include "query/expr/column_expr.h"
#include "query/physical_plan/physical_aggregate.h"
#include "query/physical_plan/physical_group_by.h"
#include "query/physical_plan/physical_seq_scan.h"
#include "query/query_test_utils.h"
#include "storage/tuple/tuple.h"
#include "test_utils.h"
#include "type/type.h"
#include "type/type_id.h"

#include <cstdlib>
#include <memory>
#include <unordered_set>

using namespace naivedb;

int main() {
    remove("test.db");
    io::DiskManager dm("test.db");
    buffer::BufferManager bm(16, &dm);
    catalog::Catalog catalog(&bm);

    auto table_info = build_test_table3(&bm, &catalog);
    auto table_schema = table_info.schema();
    auto table_id = table_info.table_id();

    auto course_col_id = table_schema->column_id("course");
    auto &course_col = table_schema->column(course_col_id);

    auto grade_col_id = table_schema->column_id("grade");
    auto &grade_col = table_schema->column(grade_col_id);

    auto seq_scan = std::make_unique<query::PhysicalSeqScan>(table_schema, table_id);
    auto group_by = std::make_unique<query::PhysicalGroupBy>(table_schema, std::move(seq_scan), course_col_id);

    std::vector<std::unique_ptr<const query::Expr>> aggregate_exprs;
    aggregate_exprs.emplace_back(std::make_unique<query::ColumnExpr>(course_col_id, course_col.type()));

    auto grade = std::make_unique<query::ColumnExpr>(grade_col_id, grade_col.type());
    aggregate_exprs.emplace_back(
        std::make_unique<query::AggregateExpr>(query::AggregateOperator::Max, grade_col.type(), std::move(grade)));

    auto output_schema = catalog::Schema({
        {"course", type::Type(type::Char(10))},
        {"MAX(grade)", type::Type(type::Int())},
    });

    // SELECT course, MAX(grade) FROM Grade GROUP BY course;
    auto aggregate =
        std::make_unique<query::PhysicalAggregate>(&output_schema, std::move(group_by), std::move(aggregate_exprs));

    query::ExecutionEngine engine(&bm, &catalog);
    auto result = engine.execute(aggregate.get());

    TEST_ASSERT_EQ(result.size(), 3);

    std::unordered_map<type::Value, type::Value> expect{{type::Value(10, "Chemistry"), type::Value(100)},
                                                        {type::Value(10, "Physics"), type::Value(95)},
                                                        {type::Value(10, "Math"), type::Value(90)}};

    for (size_t i = 0; i < result.size(); ++i) {
        auto values = result[i].values(&output_schema);
        auto iter = expect.find(values[0]);
        TEST_ASSERT_NE(iter, expect.end());
        TEST_ASSERT_EQ(values[1], iter->second);
        expect.erase(iter);
        fmt::print("{}\n", fmt::join(values, ", "));
    }

    return EXIT_SUCCESS;
}