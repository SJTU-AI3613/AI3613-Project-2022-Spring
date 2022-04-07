#include "buffer/buffer_manager.h"
#include "common/constants.h"
#include "common/types.h"
#include "io/disk_manager.h"
#include "storage/table/table_heap.h"
#include "storage/tuple/tuple.h"
#include "storage/tuple/tuple_id.h"
#include "test_utils.h"

#include <algorithm>
#include <cstdlib>
#include <fmt/core.h>
#include <iostream>
#include <random>

using namespace naivedb;

// the size of the tuple cannot exceed PAGE_SIZE - HEADER_SIZE - SLOT_SIZE = 4048
constexpr size_t MAX_DATASIZE = 4048;

constexpr size_t TUPLE_COUNT = 100;

std::vector<char> generate_random_data(std::mt19937 &rng, size_t size) {
    std::vector<char> data(size);
    for (auto &c : data) {
        c = static_cast<char>(rng());
    }
    return data;
}

uint32_t crc32(const char *data, size_t size) {
    uint32_t crc = 0;
    for (size_t i = 0; i < size; ++i) {
        crc ^= ((uint32_t)data[i] << 24);
        for (int j = 0; j < 8; ++j) {
            if ((crc & 0x80000000) != 0) {
                crc = (uint32_t)((crc << 1) ^ 0x04C11DB7);
            } else {
                crc <<= 1;
            }
        }
    }
    return crc;
}

int main() {
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<size_t> dist1(0, MAX_DATASIZE);
    std::uniform_real_distribution<double> dist2;

    std::vector<storage::Tuple> tuples(TUPLE_COUNT);
    std::vector<tuple_id_t> tuple_ids(TUPLE_COUNT);

    fmt::print("generate random tuples...\n");
    for (auto &tuple : tuples) {
        auto p = dist2(rng);
        // add some empty tuples
        if (p < 0.1) {
            tuple = storage::Tuple();
        }
        // add some tuples with MAX_DATASIZE
        else if (p < 0.2) {
            tuple = storage::Tuple(generate_random_data(rng, MAX_DATASIZE));
        }
        // add some tuples with random size
        else {
            tuple = storage::Tuple(generate_random_data(rng, dist1(rng)));
        }
    }

    uint32_t crc_sum = 0;
    for (auto &tuple : tuples) {
        crc_sum += crc32(tuple.data().data(), tuple.size());
    }

    page_id_t root_page_id;
    remove("test.db");
    {
        io::DiskManager dm("test.db");
        buffer::BufferManager bm(16, &dm);
        storage::TableHeap table(&bm);
        root_page_id = table.root_page_id();

        fmt::print("1. insert tuples into the table...\n");
        for (size_t i = 0; i < TUPLE_COUNT; ++i) {
            tuple_ids[i] = table.insert_tuple(tuples[i]);
            TEST_ASSERT_NE(tuple_ids[i], INVALID_TUPLE_ID);
            auto [page_id, slot_id] = storage::TupleId(tuple_ids[i]).page_id_and_slot_id();
            fmt::print("insert tuple {} (size {}) into page {} slot {}\n", i, tuples[i].size(), page_id, slot_id);
        }

        fmt::print("2. validate tuple values...\n");
        for (size_t i = 0; i < TUPLE_COUNT; ++i) {
            auto tuple = table.get_tuple(tuple_ids[i]);
            TEST_ASSERT_NE(tuple, std::nullopt);
            TEST_ASSERT_EQ(*tuple, tuples[i]);
        }

        fmt::print("3. validate tuple values using iterator...\n");
        uint32_t validate_crc_sum = 0;
        for (auto tuple : table) {
            validate_crc_sum += crc32(tuple.data().data(), tuple.size());
        }
        TEST_ASSERT_EQ(validate_crc_sum, crc_sum);

        fmt::print("4. delete some tuples...\n");
        size_t try_delete_count = TUPLE_COUNT / 4;
        size_t actual_delete_count = 0;
        bool deleted[TUPLE_COUNT] = {false};
        for (size_t i = 0; i < try_delete_count; ++i) {
            size_t to_delete = rng() % TUPLE_COUNT;
            if (!deleted[to_delete]) {
                TEST_ASSERT(table.delete_tuple(tuple_ids[to_delete]));
                deleted[to_delete] = true;
                ++actual_delete_count;
                fmt::print("delete tuple {}\n", to_delete);
            } else {
                TEST_ASSERT(!table.delete_tuple(tuple_ids[to_delete]));
            }
        }

        fmt::print("5. validate tuple values...\n");
        for (size_t i = 0; i < TUPLE_COUNT; ++i) {
            auto tuple = table.get_tuple(tuple_ids[i]);
            if (deleted[i]) {
                TEST_ASSERT_EQ(tuple, std::nullopt);
            } else {
                TEST_ASSERT_NE(tuple, std::nullopt);
                TEST_ASSERT_EQ(*tuple, tuples[i]);
            }
        }

        fmt::print("6. validate tuple count using iterator...\n");
        size_t tuple_count = 0;
        for (auto _ : table) {
            ++tuple_count;
        }
        TEST_ASSERT_EQ(tuple_count, TUPLE_COUNT - actual_delete_count);

        fmt::print("7. insert deleted tuples into the table...\n");
        for (size_t i = 0; i < TUPLE_COUNT; ++i) {
            if (deleted[i]) {
                tuple_ids[i] = table.insert_tuple(tuples[i]);
                TEST_ASSERT_NE(tuple_ids[i], INVALID_TUPLE_ID);
            }
        }

        fmt::print("8. validate tuple values...\n");
        for (size_t i = 0; i < TUPLE_COUNT; ++i) {
            auto tuple = table.get_tuple(tuple_ids[i]);
            TEST_ASSERT_NE(tuple, std::nullopt);
            TEST_ASSERT_EQ(*tuple, tuples[i]);
        }

        fmt::print("9. validate tuple values using iterator...\n");
        validate_crc_sum = 0;
        for (auto tuple : table) {
            validate_crc_sum += crc32(tuple.data().data(), tuple.size());
        }
        TEST_ASSERT_EQ(validate_crc_sum, crc_sum);

        fmt::print("10. update some tuples...\n");
        size_t update_count = TUPLE_COUNT / 4;
        for (size_t i = 0; i < update_count; ++i) {
            size_t to_update = rng() % TUPLE_COUNT;
            tuples[to_update] = storage::Tuple(generate_random_data(rng, tuples[to_update].size()));
            TEST_ASSERT(table.update_tuple(tuple_ids[to_update], tuples[to_update]));
        }

        fmt::print("11. validate tuple values...\n");
        for (size_t i = 0; i < TUPLE_COUNT; ++i) {
            auto tuple = table.get_tuple(tuple_ids[i]);
            TEST_ASSERT_NE(tuple, std::nullopt);
            TEST_ASSERT_EQ(*tuple, tuples[i]);
        }

        fmt::print("12. validate tuple values using iterator...\n");
        crc_sum = 0;
        for (auto &tuple : tuples) {
            crc_sum += crc32(tuple.data().data(), tuple.size());
        }

        validate_crc_sum = 0;
        for (auto tuple : table) {
            validate_crc_sum += crc32(tuple.data().data(), tuple.size());
        }
        TEST_ASSERT_EQ(validate_crc_sum, crc_sum);
    }

    fmt::print("13. check table persistence...\n");
    {
        io::DiskManager dm("test.db");
        buffer::BufferManager bm(8, &dm);
        storage::TableHeap table(&bm, root_page_id);

        for (size_t i = 0; i < TUPLE_COUNT; ++i) {
            auto tuple = table.get_tuple(tuple_ids[i]);
            TEST_ASSERT_NE(tuple, std::nullopt);
            TEST_ASSERT_EQ(*tuple, tuples[i]);
        }

        uint32_t validate_crc_sum = 0;
        for (auto tuple : table) {
            validate_crc_sum += crc32(tuple.data().data(), tuple.size());
        }
        TEST_ASSERT_EQ(validate_crc_sum, crc_sum);
    }

    return EXIT_SUCCESS;
}