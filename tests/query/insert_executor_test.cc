#include "buffer/buffer_manager.h"
#include "catalog/catalog.h"
#include "common/utils.h"
#include "io/disk_manager.h"
#include "query/execution/execution_engine.h"
#include "query/physical_plan/physical_insert.h"
#include "query/physical_plan/physical_seq_scan.h"
#include "query_test_utils.h"
#include "storage/tuple/tuple.h"
#include "test_utils.h"
#include "type/type.h"
#include "type/type_id.h"

#include <cstdlib>
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

    // INSERT INTO Person VALUES (4, 'Eve', 21), (5, 'Frank', 22);
    auto insert = std::make_unique<query::PhysicalInsert>(
        make_vector(make_vector(type::Value(4), type::Value(20, "Eve"), type::Value(21)),
                    make_vector(type::Value(5), type::Value(20, "Frank"), type::Value(22))),
        table_id);

    // SELECT * FROM Person;
    auto seq_scan = std::make_unique<query::PhysicalSeqScan>(table_schema, table_id);

    query::ExecutionEngine engine(&bm, &catalog);
    auto result = engine.execute(insert.get());
    TEST_ASSERT_EQ(result.size(), 0);

    result = engine.execute(seq_scan.get());

    auto expect = make_vector(make_vector(type::Value(0), type::Value(20, "Alice"), type::Value(17)),
                              make_vector(type::Value(1), type::Value(20, "Bob"), type::Value(18)),
                              make_vector(type::Value(2), type::Value(20, "Carol"), type::Value(19)),
                              make_vector(type::Value(3), type::Value(20, "Dave"), type::Value(20)),
                              make_vector(type::Value(4), type::Value(20, "Eve"), type::Value(21)),
                              make_vector(type::Value(5), type::Value(20, "Frank"), type::Value(22)));

    TEST_ASSERT_EQ(result.size(), expect.size());

    for (size_t i = 0; i < result.size(); ++i) {
        TEST_ASSERT_EQ(result[i], expect[i]);
        fmt::print("{}\n", fmt::join(result[i].values(table_schema), ", "));
    }

    return EXIT_SUCCESS;
}