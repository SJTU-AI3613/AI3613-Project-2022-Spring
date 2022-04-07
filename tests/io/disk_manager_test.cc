#include "common/constants.h"
#include "common/types.h"
#include "io/disk_manager.h"
#include "test_utils.h"

#include <cstring>

using namespace naivedb;

int main() {
    char data1[PAGE_SIZE] = "hello, world!";
    char data2[PAGE_SIZE] = "hello, naivedb!";
    char buf[PAGE_SIZE];

    remove("test.db");
    page_id_t page1, page2;
    {
        auto dm = io::DiskManager("test.db");

        page1 = dm.alloc_page();
        TEST_ASSERT(dm.page_allocated(page1));

        dm.write_page(page1, data1);
        dm.read_page(page1, buf);
        TEST_ASSERT_EQ(std::memcmp(data1, buf, strlen(data1) + 1), 0);

        dm.free_page(page1);
        TEST_ASSERT(!dm.page_allocated(page1));

        page2 = dm.alloc_page();
        TEST_ASSERT_EQ(page1, page2);
        dm.write_page(page2, data2);

        page1 = dm.alloc_page();
        TEST_ASSERT_NE(page1, page2);
        dm.free_page(page1);
    }

    {
        auto dm = io::DiskManager("test.db");

        TEST_ASSERT(!dm.page_allocated(page1));
        TEST_ASSERT(dm.page_allocated(page2));

        dm.read_page(page2, buf);
        TEST_ASSERT_EQ(std::memcmp(data2, buf, strlen(data2) + 1), 0);

        page2 = dm.alloc_page();
        TEST_ASSERT_EQ(page1, page2);
    }
    return EXIT_SUCCESS;
}
