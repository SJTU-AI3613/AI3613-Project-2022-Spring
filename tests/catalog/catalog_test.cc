#include "buffer/buffer_manager.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "catalog/table_info.h"
#include "common/constants.h"
#include "io/disk_manager.h"
#include "test_utils.h"
#include "type/type.h"
#include "type/type_id.h"

#include <cstdlib>

using namespace naivedb;

int main() {
    remove("test.db");
    io::DiskManager dm("test.db");
    buffer::BufferManager bm(16, &dm);
    catalog::Catalog catalog(&bm);

    TEST_ASSERT_EQ(catalog.get_table_id("tab_1"), INVALID_TABLE_ID);

    auto table_id = catalog.create_table("tab_1",
                                         catalog::Schema({
                                             {"col_1", type::Type(type::Int())},
                                             {"col_2", type::Type(type::Char(3))},
                                         }));
    TEST_ASSERT_NE(table_id, INVALID_TABLE_ID);

    auto table_id_ = catalog.get_table_id("tab_1");
    TEST_ASSERT_EQ(table_id, table_id_);

    auto table_info = catalog.get_table_info(table_id);
    TEST_ASSERT_EQ(table_info.table_name(), "tab_1");
    TEST_ASSERT_EQ(*table_info.schema(),
                   catalog::Schema({
                       {"col_1", type::Type(type::Int())},
                       {"col_2", type::Type(type::Char(3))},
                   }));

    return EXIT_SUCCESS;
}