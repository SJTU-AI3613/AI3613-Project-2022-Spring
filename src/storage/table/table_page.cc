#include "storage/table/table_page.h"

#include "common/constants.h"
#include "storage/tuple/tuple.h"
#include "storage/tuple/tuple_id.h"

#include <cstdint>

namespace naivedb::storage {
void TablePage::init(page_id_t prev_page_id) {
    page_.clear();
    set_lsn(INVALID_LSN);
    set_prev_page_id(prev_page_id);
    set_next_page_id(INVALID_PAGE_ID);
    set_free_space_pointer(PAGE_SIZE);
    set_slot_count(0);
    set_tuple_count(0);
}

slot_id_t TablePage::insert_tuple(const Tuple &tuple) {
    // return INVALID_SLOT_ID if there is not enough space
    if (free_space() < tuple.size() + SLOT_SIZE) {
        return INVALID_SLOT_ID;
    }

    // try to find a free slot
    slot_id_t slot_id;
    for (slot_id = 0; slot_id < slot_count(); ++slot_id) {
        if (tuple_deleted(slot_id)) {
            break;
        }
    }

    // insert the tuple
    set_free_space_pointer(free_space_pointer() - tuple.size());
    std::memcpy(page_.data_mut() + free_space_pointer(), tuple.data().data(), tuple.size());

    set_tuple_offset(slot_id, free_space_pointer());
    set_tuple_size(slot_id, tuple.size());
    if (slot_id == slot_count()) {
        set_slot_count(slot_count() + 1);
    }
    set_tuple_count(tuple_count() + 1);
    return slot_id;
}

bool TablePage::delete_tuple(slot_id_t slot_id) {
    if (slot_id >= slot_count()) {
        return false;
    }
    if (tuple_deleted(slot_id)) {
        return false;
    }
    auto tuple_offset = this->tuple_offset(slot_id);
    auto tuple_size = this->tuple_size(slot_id);
    auto free_space_pointer = this->free_space_pointer();

    // shift other tuples
    std::memmove(page_.data_mut() + free_space_pointer + tuple_size,
                 page_.data() + free_space_pointer,
                 tuple_offset - free_space_pointer);

    set_free_space_pointer(free_space_pointer + tuple_size);
    set_tuple_offset(slot_id, 0);
    set_tuple_size(slot_id, 0);
    set_tuple_count(tuple_count() - 1);

    // update other slots
    for (slot_id_t i = 0; i < slot_count(); ++i) {
        if (!tuple_deleted(i) && this->tuple_offset(i) < tuple_offset) {
            set_tuple_offset(i, this->tuple_offset(i) + tuple_size);
        }
    }
    return true;
}

std::optional<Tuple> TablePage::get_tuple(slot_id_t slot_id) const {
    if (slot_id >= slot_count()) {
        return std::nullopt;
    }
    if (tuple_deleted(slot_id)) {
        return std::nullopt;
    }
    auto tuple_offset = this->tuple_offset(slot_id);
    auto tuple_size = this->tuple_size(slot_id);

    std::vector<char> tuple_data(page_.data() + tuple_offset, page_.data() + tuple_offset + tuple_size);
    return Tuple(std::move(tuple_data));
}

bool TablePage::update_tuple(slot_id_t slot_id, const Tuple &tuple) {
    if (slot_id >= slot_count()) {
        return false;
    }
    if (tuple_deleted(slot_id)) {
        return false;
    }

    auto tuple_offset = this->tuple_offset(slot_id);
    auto tuple_size = this->tuple_size(slot_id);
    // only fixed-length tuple is allowed so far.
    if (tuple.size() != tuple_size) {
        return false;
    }

    std::memcpy(page_.data_mut() + tuple_offset, tuple.data().data(), tuple_size);
    return true;
}

slot_id_t TablePage::first_slot() const {
    for (slot_id_t slot_id = 0; slot_id < slot_count(); ++slot_id) {
        if (!tuple_deleted(slot_id)) {
            return slot_id;
        }
    }
    return INVALID_SLOT_ID;
}

slot_id_t TablePage::next_slot(slot_id_t slot_id) const {
    for (++slot_id; slot_id < slot_count(); ++slot_id) {
        if (!tuple_deleted(slot_id)) {
            return slot_id;
        }
    }
    return INVALID_SLOT_ID;
}

slot_id_t TablePage::prev_slot(slot_id_t slot_id) const {
    for (--slot_id; slot_id >= 0; --slot_id) {
        if (!tuple_deleted(slot_id)) {
            return slot_id;
        }
    }
    return INVALID_SLOT_ID;
}

slot_id_t TablePage::last_slot() const {
    for (slot_id_t slot_id = slot_count() - 1; slot_id >= 0; --slot_id) {
        if (!tuple_deleted(slot_id)) {
            return slot_id;
        }
    }
    return INVALID_SLOT_ID;
}
}  // namespace naivedb::storage