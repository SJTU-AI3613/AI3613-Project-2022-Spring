#pragma once

#include "common/constants.h"
#include "common/format.h"
#include "common/types.h"

#include <cassert>
#include <vector>

namespace naivedb::log {
enum class LogRecordType {
    Invalid = 0,
    Update,
    Begin,
    Commit,
    Abort,
};

class LogRecord {
    friend struct fmt::formatter<naivedb::log::LogRecord>;
    struct Header {
        Header(LogRecordType type, uint32_t size, txn_id_t txn_id, lsn_t prev_lsn)
            : type_(type), size_(size), txn_id_(txn_id), prev_lsn_(prev_lsn) {}
        LogRecordType type_;
        uint32_t size_;
        txn_id_t txn_id_;
        lsn_t prev_lsn_;

        bool operator==(const Header &other) const {
            return type_ == other.type_ && size_ == other.size_ && txn_id_ == other.txn_id_ &&
                   prev_lsn_ == other.prev_lsn_;
        }

        bool operator!=(const Header &other) const { return !(*this == other); }
    };

  public:
    LogRecord(LogRecordType type, txn_id_t txn_id, lsn_t prev_lsn)
        : header_(type, HEADER_SIZE, txn_id, prev_lsn), page_id_(INVALID_PAGE_ID), slot_id_(INVALID_SLOT_ID) {
        assert(type != LogRecordType::Update);
    }

    LogRecord(LogRecordType type,
              txn_id_t txn_id,
              lsn_t prev_lsn,
              page_id_t page_id,
              slot_id_t slot_id,
              std::vector<char> &&old_data,
              std::vector<char> &&new_data)
        : header_(type, 0, txn_id, prev_lsn)
        , page_id_(page_id)
        , slot_id_(slot_id)
        , old_data_(std::move(old_data))
        , new_data_(std::move(new_data)) {
        assert(type == LogRecordType::Update);
        assert(old_data_.size() == new_data_.size());
        header_.size_ =
            HEADER_SIZE + sizeof(page_id_t) + sizeof(slot_id_t) + sizeof(uint32_t) * 2 + old_data_.size() * 2;
    }

    bool operator==(const LogRecord &other) const {
        return header_ == other.header_ && page_id_ == other.page_id_ && slot_id_ == other.slot_id_ &&
               old_data_ == other.old_data_ && new_data_ == other.new_data_;
    }

    bool operator!=(const LogRecord &other) const { return !(*this == other); }

    LogRecordType type() const { return header_.type_; }

    size_t size() const { return header_.size_; }

    txn_id_t txn_id() const { return header_.txn_id_; }

    lsn_t prev_lsn() const { return header_.prev_lsn_; }

    page_id_t page_id() const { return page_id_; }

    slot_id_t slot_id() const { return slot_id_; }

    const std::vector<char> &old_data() const { return old_data_; }

    const std::vector<char> &new_data() const { return new_data_; }

    void serialize(char *buffer) const;

    static LogRecord deserialize(const char *buffer);

  private:
    static constexpr size_t HEADER_SIZE = sizeof(Header);
    Header header_;

    // for update
    page_id_t page_id_;
    slot_id_t slot_id_;
    std::vector<char> old_data_;
    std::vector<char> new_data_;
};
}  // namespace naivedb::log

namespace fmt {
template <>
struct formatter<naivedb::log::LogRecordType> : public naivedb_base_formatter {
    template <typename FormatContext>
    auto format(const naivedb::log::LogRecordType &obj, FormatContext &ctx) -> decltype(ctx.out()) {
        switch (obj) {
            case naivedb::log::LogRecordType::Invalid:
                return format_to(ctx.out(), "Invalid");

            case naivedb::log::LogRecordType::Update:
                return format_to(ctx.out(), "Update");

            case naivedb::log::LogRecordType::Begin:
                return format_to(ctx.out(), "Begin");

            case naivedb::log::LogRecordType::Commit:
                return format_to(ctx.out(), "Commit");

            case naivedb::log::LogRecordType::Abort:
                return format_to(ctx.out(), "Abort");
        }
        return format_to(ctx.out(), "");
    }
};

template <>
struct formatter<naivedb::log::LogRecord::Header> : public naivedb_base_formatter {
    template <typename FormatContext>
    auto format(const naivedb::log::LogRecord::Header &obj, FormatContext &ctx) -> decltype(ctx.out()) {
        return format_to(ctx.out(),
                         "Header {{ type_: {}, size_: {}, txn_id_: {}, prev_lsn_: {} }}",
                         obj.type_,
                         obj.size_,
                         obj.txn_id_,
                         obj.prev_lsn_);
    }
};

template <>
struct formatter<naivedb::log::LogRecord> : public naivedb_base_formatter {
    template <typename FormatContext>
    auto format(const naivedb::log::LogRecord &obj, FormatContext &ctx) -> decltype(ctx.out()) {
        return format_to(
            ctx.out(),
            "LogRecord {{ header_: {}, page_id_: {}, slot_id_: {}, old_data_: [{:#x}], new_data_: [{:#x}] }}",
            obj.header_,
            obj.page_id_,
            obj.slot_id_,
            join(obj.old_data_, ", "),
            join(obj.new_data_, ", "));
    }
};
}  // namespace fmt