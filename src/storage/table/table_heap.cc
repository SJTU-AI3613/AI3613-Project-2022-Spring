#include "storage/table/table_heap.h"

#include "buffer/buffer_manager.h"
#include "common/constants.h"
#include "common/exception.h"
#include "common/types.h"
#include "storage/table/table_page.h"
#include "storage/tuple/tuple.h"
#include "storage/tuple/tuple_id.h"

#include <cassert>
#include <optional>

namespace naivedb::storage {
TableHeap::TableHeap(buffer::BufferManager *buffer_manager) : buffer_manager_(buffer_manager) {
    auto page = buffer_manager->new_page();
    assert(page);
    root_page_id_ = page->page_id();
    TablePage(*std::move(page)).init(INVALID_PAGE_ID);
}

TableHeap::TableHeap(buffer::BufferManager *buffer_manager, page_id_t root_page_id)
    : buffer_manager_(buffer_manager), root_page_id_(root_page_id) {}

tuple_id_t TableHeap::insert_tuple(const Tuple &tuple) {
    auto root_page = buffer_manager_->fetch_page(root_page_id_);
    if (!root_page) {
        return INVALID_TUPLE_ID;
    }
    auto current_table_page = TablePage(*std::move(root_page));

    slot_id_t slot_id;
    while ((slot_id = current_table_page.insert_tuple(tuple)) == INVALID_SLOT_ID) {
        auto next_page_id = current_table_page.next_page_id();
        // if the next page is a valid page
        if (next_page_id != INVALID_PAGE_ID) {
            // unpin the current page and try to insert the tuple in the next page
            auto next_page = buffer_manager_->fetch_page(next_page_id);
            if (!next_page) {
                return INVALID_TUPLE_ID;
            }
            current_table_page = TablePage(*std::move(next_page));
        }
        // otherwise create a new page and try to insert the tuple in it
        else {
            auto new_page = buffer_manager_->new_page();
            if (!new_page) {
                return INVALID_TUPLE_ID;
            }
            auto new_table_page = TablePage(*std::move(new_page));
            current_table_page.set_next_page_id(new_table_page.page_id());
            new_table_page.init(current_table_page.page_id());
            current_table_page = std::move(new_table_page);
        }
    }
    return TupleId(current_table_page.page_id(), slot_id).tuple_id();
}

bool TableHeap::delete_tuple(tuple_id_t tuple_id) {
    auto [page_id, slot_id] = TupleId(tuple_id).page_id_and_slot_id();
    auto page = buffer_manager_->fetch_page(page_id);
    if (!page) {
        return false;
    }
    uint32_t tuple_count;
    page_id_t prev_page_id, next_page_id;
    {
        auto table_page = TablePage(*std::move(page));
        if (!table_page.delete_tuple(slot_id)) {
            return false;
        }
        tuple_count = table_page.tuple_count();
        prev_page_id = table_page.prev_page_id();
        next_page_id = table_page.next_page_id();
    }
    // delete the page if it is empty
    if (tuple_count == 0 && page_id != root_page_id_) {
        auto prev_page = buffer_manager_->fetch_page(prev_page_id);
        if (!prev_page) {
            return false;
        }
        auto prev_table_page = TablePage(*std::move(prev_page));
        prev_table_page.set_next_page_id(next_page_id);
        if (next_page_id != INVALID_PAGE_ID) {
            auto next_page = buffer_manager_->fetch_page(next_page_id);
            if (!next_page) {
                return false;
            }
            auto next_table_page = TablePage(*std::move(next_page));
            next_table_page.set_prev_page_id(prev_page_id);
        }
        if (!buffer_manager_->delete_page(page_id)) {
            return false;
        }
    }
    return true;
}

std::optional<Tuple> TableHeap::get_tuple(tuple_id_t tuple_id) {
    auto [page_id, slot_id] = TupleId(tuple_id).page_id_and_slot_id();
    auto page = buffer_manager_->fetch_page(page_id);
    if (!page) {
        return std::nullopt;
    }
    return TablePage(*std::move(page)).get_tuple(slot_id);
}

bool TableHeap::update_tuple(tuple_id_t tuple_id, const Tuple &tuple) {
    auto [page_id, slot_id] = TupleId(tuple_id).page_id_and_slot_id();
    auto page = buffer_manager_->fetch_page(page_id);
    if (!page) {
        return false;
    }
    return TablePage(*std::move(page)).update_tuple(slot_id, tuple);
}

TableHeap::Iterator TableHeap::begin() {
    auto page = buffer_manager_->fetch_page(root_page_id_);
    assert(page);

    auto table_page = TablePage(*std::move(page));
    auto slot_id = table_page.first_slot();
    // if root page is empty, find the first slot in the next page
    if (slot_id == INVALID_SLOT_ID) {
        auto next_page_id = table_page.next_page_id();
        if (next_page_id == INVALID_PAGE_ID) {
            return Iterator(this, INVALID_TUPLE_ID);
        }
        auto next_page = buffer_manager_->fetch_page(next_page_id);
        assert(next_page);
        // except the root page, other pages must be non-empty
        slot_id = TablePage(*std::move(next_page)).first_slot();
        return Iterator(this, TupleId(next_page_id, slot_id).tuple_id());
    }
    return Iterator(this, TupleId(root_page_id_, slot_id).tuple_id());
}

TableHeap::Iterator TableHeap::end() { return Iterator(this, INVALID_TUPLE_ID); }

TableHeap::Iterator &TableHeap::Iterator::operator++() {
    auto [page_id, slot_id] = TupleId(tuple_id_).page_id_and_slot_id();
    auto page = table_heap_->buffer_manager_->fetch_page(page_id);
    assert(page);

    auto table_page = TablePage(*std::move(page));
    auto next_slot_id = table_page.next_slot(slot_id);
    // go to the next page
    if (next_slot_id == INVALID_SLOT_ID) {
        auto next_page_id = table_page.next_page_id();
        if (next_page_id == INVALID_PAGE_ID) {
            tuple_id_ = INVALID_TUPLE_ID;
            return *this;
        }
        auto next_page = table_heap_->buffer_manager_->fetch_page(next_page_id);
        assert(next_page);

        next_slot_id = TablePage(*std::move(next_page)).first_slot();
        tuple_id_ = TupleId(next_page_id, next_slot_id).tuple_id();
        return *this;
    }
    tuple_id_ = TupleId(page_id, next_slot_id).tuple_id();
    return *this;
}

TableHeap::Iterator TableHeap::Iterator::operator++(int) {
    auto old = *this;
    ++*this;
    return old;
}

TableHeap::Iterator &TableHeap::Iterator::operator--() {
    auto [page_id, slot_id] = TupleId(tuple_id_).page_id_and_slot_id();
    auto page = table_heap_->buffer_manager_->fetch_page(page_id);
    assert(page);

    auto table_page = TablePage(*std::move(page));
    auto prev_slot_id = table_page.prev_slot(slot_id);
    // go to the previous page
    if (prev_slot_id == INVALID_SLOT_ID) {
        auto prev_page_id = table_page.prev_page_id();
        if (prev_page_id == INVALID_PAGE_ID) {
            tuple_id_ = INVALID_TUPLE_ID;
            return *this;
        }
        auto prev_page = table_heap_->buffer_manager_->fetch_page(prev_page_id);
        assert(prev_page);

        prev_slot_id = TablePage(*std::move(prev_page)).last_slot();
        tuple_id_ = TupleId(prev_page_id, prev_slot_id).tuple_id();
        return *this;
    }
    tuple_id_ = TupleId(page_id, prev_slot_id).tuple_id();
    return *this;
}

TableHeap::Iterator TableHeap::Iterator::operator--(int) {
    auto old = *this;
    --*this;
    return old;
}

Tuple TableHeap::Iterator::operator*() { return *table_heap_->get_tuple(tuple_id_); }
}  // namespace naivedb::storage