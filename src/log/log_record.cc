#include "log/log_record.h"

#include "common/types.h"

#include <cstdint>
#include <cstring>
#include <vector>

namespace naivedb::log {
void LogRecord::serialize(char *buffer) const {
    *reinterpret_cast<Header *>(buffer) = header_;
    if (header_.type_ == LogRecordType::Update) {
        buffer += HEADER_SIZE;
        *reinterpret_cast<page_id_t *>(buffer) = page_id_;
        buffer += sizeof(page_id_t);
        *reinterpret_cast<slot_id_t *>(buffer) = slot_id_;
        buffer += sizeof(slot_id_t);
        *reinterpret_cast<uint32_t *>(buffer) = old_data_.size();
        buffer += sizeof(uint32_t);
        std::memcpy(buffer, old_data_.data(), old_data_.size());
        buffer += old_data_.size();
        *reinterpret_cast<uint32_t *>(buffer) = new_data_.size();
        buffer += sizeof(uint32_t);
        std::memcpy(buffer, new_data_.data(), new_data_.size());
    }
}

LogRecord LogRecord::deserialize(const char *buffer) {
    Header header = *reinterpret_cast<const Header *>(buffer);
    if (header.type_ != LogRecordType::Update) {
        return LogRecord(header.type_, header.txn_id_, header.prev_lsn_);
    }
    buffer += HEADER_SIZE;
    auto page_id = *reinterpret_cast<const page_id_t *>(buffer);
    buffer += sizeof(page_id_t);
    auto slot_id = *reinterpret_cast<const slot_id_t *>(buffer);
    buffer += sizeof(slot_id_t);
    std::vector<char> old_data;
    std::vector<char> new_data;
    auto old_data_size = *reinterpret_cast<const uint32_t *>(buffer);
    buffer += sizeof(uint32_t);
    old_data.resize(old_data_size);
    std::memcpy(old_data.data(), buffer, old_data_size);
    buffer += old_data_size;
    auto new_data_size = *reinterpret_cast<const uint32_t *>(buffer);
    buffer += sizeof(uint32_t);
    new_data.resize(new_data_size);
    std::memcpy(new_data.data(), buffer, new_data_size);
    assert(old_data_size == new_data_size);
    return LogRecord(
        header.type_, header.txn_id_, header.prev_lsn_, page_id, slot_id, std::move(old_data), std::move(new_data));
}
}  // namespace naivedb::log