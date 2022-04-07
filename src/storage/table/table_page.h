#pragma once

#include "common/macros.h"
#include "common/types.h"
#include "storage/page/page_guard.h"

#include <cstdint>
#include <optional>

namespace naivedb {
namespace storage {
class Tuple;
}
}  // namespace naivedb

namespace naivedb::storage {
/**
 * @brief TablePage is the representation of table pages with slotted page layout.
 *
 * Page layout:
 *  --------------------------------------------------------------------------------------------------------------
 * | Header (40) | Tuple_0_offset (4) | Tuple_0_size (4) | ... | Free space | Tuple_N | Tuple_N-1 | ... | Tuple_0 |
 *  --------------------------------------------------------------------------------------------------------------
 *               |<-------------- Slot Array ----------------->|            |<- free space pointer
 *
 * Header layout:
 !*  ---------------------------------------------------------------------------------------------------------------------------
 !* | lsn (8) | prev_page_id (8) | next_page_id (8) | free_space_pointer (4) | slot_count (4) | tuple_count (4) | (padding) (4) |
 !*  ---------------------------------------------------------------------------------------------------------------------------
 */
class TablePage {
    DISALLOW_COPY(TablePage)

    struct Header {
        lsn_t lsn_;
        page_id_t prev_page_id_;
        page_id_t next_page_id_;
        uint32_t free_space_pointer_;
        uint32_t slot_count_;
        uint32_t tuple_count_;
    };

    struct Slot {
        uint32_t offset_;
        uint32_t size_;
    };

    static_assert(sizeof(Header) == 40);
    static_assert(sizeof(Slot) == 8);

  public:
    explicit TablePage(PageGuard &&raw_page) : page_(std::move(raw_page)) {}

    TablePage(TablePage &&table_page) : page_(std::move(table_page.page_)) {}

    TablePage &operator=(TablePage &&table_page) {
        page_ = std::move(table_page.page_);
        return *this;
    }

    ~TablePage() = default;

    void init(page_id_t prev_page_id);

    slot_id_t insert_tuple(const Tuple &tuple);

    bool delete_tuple(slot_id_t slot_id);

    std::optional<Tuple> get_tuple(slot_id_t slot_id) const;

    bool update_tuple(slot_id_t slot_id, const Tuple &tuple);

    slot_id_t first_slot() const;

    slot_id_t next_slot(slot_id_t slot_id) const;

    slot_id_t prev_slot(slot_id_t slot_id) const;

    slot_id_t last_slot() const;

    page_id_t page_id() const { return page_.page_id(); }

    lsn_t lsn() const { return header()->lsn_; }
    void set_lsn(lsn_t lsn) { header()->lsn_ = lsn; }

    page_id_t prev_page_id() const { return header()->prev_page_id_; }
    void set_prev_page_id(page_id_t prev_page_id) { header()->prev_page_id_ = prev_page_id; }

    page_id_t next_page_id() const { return header()->next_page_id_; }
    void set_next_page_id(page_id_t next_page_id) { header()->next_page_id_ = next_page_id; }

    uint32_t tuple_count() const { return header()->tuple_count_; }

  private:
    void set_tuple_count(uint32_t tuple_count) { header()->tuple_count_ = tuple_count; }

    uint32_t free_space() const { return free_space_pointer() - HEADER_SIZE - SLOT_SIZE * slot_count(); }

    uint32_t free_space_pointer() const { return header()->free_space_pointer_; }
    void set_free_space_pointer(uint32_t free_space_pointer) { header()->free_space_pointer_ = free_space_pointer; }

    slot_id_t slot_count() const { return header()->slot_count_; }
    void set_slot_count(slot_id_t slot_count) { header()->slot_count_ = slot_count; }

    uint32_t tuple_offset(slot_id_t slot_id) const { return slots()[slot_id].offset_; }
    void set_tuple_offset(slot_id_t slot_id, uint32_t offset) { slots()[slot_id].offset_ = offset; }

    uint32_t tuple_size(slot_id_t slot_id) const { return slots()[slot_id].size_; }
    void set_tuple_size(slot_id_t slot_id, uint32_t size) { slots()[slot_id].size_ = size; }

    bool tuple_deleted(slot_id_t slot_id) const { return tuple_offset(slot_id) == 0; }

    Header *header() { return reinterpret_cast<Header *>(page_.data_mut()); }

    const Header *header() const { return reinterpret_cast<const Header *>(page_.data()); }

    Slot *slots() { return reinterpret_cast<Slot *>(page_.data_mut() + OFFSET_SLOT_ARRAY); }

    const Slot *slots() const { return reinterpret_cast<const Slot *>(page_.data() + OFFSET_SLOT_ARRAY); }

    static constexpr size_t OFFSET_SLOT_ARRAY = sizeof(Header);
    static constexpr size_t SLOT_SIZE = sizeof(Slot);
    static constexpr size_t HEADER_SIZE = sizeof(Header);

    PageGuard page_;
};
}  // namespace naivedb::storage