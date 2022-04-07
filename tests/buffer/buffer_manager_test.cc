#include "buffer/buffer_manager.h"
#include "common/constants.h"
#include "io/disk_manager.h"
#include "storage/page/page_guard.h"
#include "test_utils.h"

#include <cstring>
#include <iostream>
#include <optional>
#include <string_view>
#include <vector>

using namespace naivedb;

int main() {
    remove("test.db");
    io::DiskManager dm("test.db");
    buffer::BufferManager bm(3, &dm);

    constexpr char zeros[PAGE_SIZE] = {0};
    std::vector<std::string_view> str{"aaaaaaaaaa", "bbbbbbbbbb", "cccccccccc"};
    page_id_t page_id[4];
    storage::PageGuard page[4];
    char buf[PAGE_SIZE];

    for (int i = 0; i < 3; ++i) {
        auto page_guard = bm.new_page();
        TEST_ASSERT_NE(page_guard, std::nullopt);
        page[i] = *std::move(page_guard);
        page_id[i] = page[i].page_id();
        TEST_ASSERT_EQ(page_id[i], i);
        TEST_ASSERT_EQ(std::memcmp(page[i].data(), zeros, PAGE_SIZE), 0);
        std::memcpy(page[i].data_mut(), str[i].data(), str[i].size());
    }

    {
        auto page_guard = bm.new_page();
        TEST_ASSERT_EQ(page_guard, std::nullopt);
    }

    { storage::PageGuard page0 = std::move(page[0]); }
    // now page[0] is unpinned
    {
        auto page_guard = bm.new_page();
        TEST_ASSERT_NE(page_guard, std::nullopt);
        page[3] = *std::move(page_guard);
        page_id[3] = page[3].page_id();
        TEST_ASSERT_EQ(page_id[3], 3);
        TEST_ASSERT_EQ(std::memcmp(page[3].data(), zeros, PAGE_SIZE), 0);
    }

    dm.read_page(page_id[0], buf);
    TEST_ASSERT_EQ(std::memcmp(buf, str[0].data(), str[0].size()), 0);
    dm.read_page(page_id[1], buf);
    TEST_ASSERT_EQ(std::memcmp(buf, zeros, PAGE_SIZE), 0);
    dm.read_page(page_id[2], buf);
    TEST_ASSERT_EQ(std::memcmp(buf, zeros, PAGE_SIZE), 0);

    {
        auto page_guard = bm.fetch_page(page_id[0]);
        TEST_ASSERT_EQ(page_guard, std::nullopt);
    }

    TEST_ASSERT(!bm.delete_page(page_id[3]));
    { storage::PageGuard page3 = std::move(page[3]); }
    // now page[3] is unpinned
    TEST_ASSERT(bm.delete_page(page_id[3]));
    TEST_ASSERT(!bm.page_allocated(page_id[3]));

    {
        auto page_guard = bm.fetch_page(page_id[0]);
        TEST_ASSERT_NE(page_guard, std::nullopt);
        page[0] = *std::move(page_guard);
        TEST_ASSERT_EQ(std::memcmp(page[0].data(), str[0].data(), str[0].size()), 0);
    }

    bm.flush_page(page_id[2]);
    dm.read_page(page_id[0], buf);
    TEST_ASSERT_EQ(std::memcmp(buf, str[0].data(), str[0].size()), 0);
    dm.read_page(page_id[1], buf);
    TEST_ASSERT_EQ(std::memcmp(buf, zeros, PAGE_SIZE), 0);
    dm.read_page(page_id[2], buf);
    TEST_ASSERT_EQ(std::memcmp(buf, str[2].data(), str[2].size()), 0);

    bm.flush_all_pages();
    dm.read_page(page_id[0], buf);
    TEST_ASSERT_EQ(std::memcmp(buf, str[0].data(), str[0].size()), 0);
    dm.read_page(page_id[1], buf);
    TEST_ASSERT_EQ(std::memcmp(buf, str[1].data(), str[1].size()), 0);
    dm.read_page(page_id[2], buf);
    TEST_ASSERT_EQ(std::memcmp(buf, str[2].data(), str[2].size()), 0);

    return EXIT_SUCCESS;
}