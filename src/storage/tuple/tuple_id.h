#pragma once

#include "common/types.h"

#include <type_traits>
#include <utility>

namespace naivedb::storage {
class TupleId {
  public:
    explicit TupleId(tuple_id_t tuple_id)
        : page_id_(static_cast<typename std::make_unsigned<tuple_id_t>::type>(tuple_id) >> PAGE_SHIFT)
        , slot_id_(tuple_id & SLOT_ID_MASK)
        , tuple_id_(tuple_id) {}
    TupleId(page_id_t page_id, slot_id_t slot_id)
        : page_id_(page_id), slot_id_(slot_id), tuple_id_((page_id << PAGE_SHIFT) | slot_id) {}

    page_id_t page_id() const { return page_id_; }
    slot_id_t slot_id() const { return slot_id_; }
    tuple_id_t tuple_id() const { return tuple_id_; }
    std::pair<page_id_t, slot_id_t> page_id_and_slot_id() const { return {page_id_, slot_id_}; }

  private:
    page_id_t page_id_;
    slot_id_t slot_id_;
    tuple_id_t tuple_id_;

    static constexpr tuple_id_t PAGE_SHIFT = 16;
    static constexpr tuple_id_t SLOT_ID_MASK = (1 << PAGE_SHIFT) - 1;
};
}  // namespace naivedb::storage