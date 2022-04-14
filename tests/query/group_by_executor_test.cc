#include "buffer/buffer_manager.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/utils.h"
#include "io/disk_manager.h"
#include "query/execution/execution_engine.h"
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

    auto seq_scan = std::make_unique<query::PhysicalSeqScan>(table_schema, table_id);

    // SELECT * FROM Grade GROUP BY course;
    auto group_by = std::make_unique<query::PhysicalGroupBy>(table_schema, std::move(seq_scan), course_col_id);

    query::ExecutionEngine engine(&bm, &catalog);
    auto result = engine.execute(group_by.get());

    TEST_ASSERT_EQ(result.size(), 9);

    std::vector<std::vector<type::Value>> outputs;

    for (size_t i = 0; i < result.size(); ++i) {
        auto values = result[i].values(table_schema);
        fmt::print("{}\n", fmt::join(values, ", "));
        outputs.emplace_back(std::move(values));
    }

    std::unordered_set<type::Value> set{
        type::Value(10, "Math"),
        type::Value(10, "Physics"),
        type::Value(10, "Chemistry"),
    };

    for (size_t i = 0; i < 9; i += 3) {
        TEST_ASSERT_EQ(outputs[i][1], outputs[i + 1][1]);
        TEST_ASSERT_EQ(outputs[i + 1][1], outputs[i + 2][1]);
        auto iter = set.find(outputs[i][1]);
        TEST_ASSERT_NE(iter, set.end());
        set.erase(iter);
    }

    return EXIT_SUCCESS;
}