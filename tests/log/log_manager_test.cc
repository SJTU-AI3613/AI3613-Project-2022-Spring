#include "naivedb_prelude.h"
#include "test_utils.h"

#include <cstring>
#include <vector>

using namespace naivedb;

int main() {
    remove("test.log.db");
    io::DiskManager dm("test.log.db");
    buffer::BufferManager bm(3, &dm);
    log::LogManager lm(&bm);

    std::vector<log::LogRecord> records{
        log::LogRecord(log::LogRecordType::Invalid, 123, 456),
        log::LogRecord(log::LogRecordType::Begin, 789, 123),
        log::LogRecord(log::LogRecordType::Commit, 456, 789),
        log::LogRecord(log::LogRecordType::Abort, 123, 456),
        log::LogRecord(log::LogRecordType::Update, 789, 123, 12345, 6789, {'a', 'b', 'c', 'd'}, {'d', 'c', 'b', 'a'}),
    };

    std::vector<lsn_t> lsns;
    for (auto &record : records) {
        auto lsn = lm.append_record(record);
        TEST_ASSERT_NE(lsn, INVALID_LSN);
        lsns.emplace_back(lsn);
    }

    for (size_t i = 0; i < records.size(); ++i) {
        TEST_ASSERT_EQ(lm.get_record(lsns[i]), records[i]);
    }

    log::LogRecord big_record(
        log::LogRecordType::Update, 123, 456, 789, 123, std::vector<char>(2022), std::vector<char>(2022));
    auto lsn = lm.append_record(big_record);
    TEST_ASSERT_NE(lsn, INVALID_LSN);
    TEST_ASSERT_EQ(lm.get_record(lsn), big_record);

    return EXIT_SUCCESS;
}