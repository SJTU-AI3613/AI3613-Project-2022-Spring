#pragma once

#include "common/constants.h"
#include "common/types.h"

#include <optional>

namespace naivedb {
namespace buffer {
class BufferManager;
}
namespace storage {
class Tuple;
}
}  // namespace naivedb

namespace naivedb::storage {
class TableHeap {
  public:
    class Iterator {
      public:
        Iterator() : table_heap_(nullptr), tuple_id_(INVALID_TUPLE_ID) {}

        Iterator(TableHeap *table_heap, tuple_id_t tuple_id) : table_heap_(table_heap), tuple_id_(tuple_id) {}

        bool operator==(const Iterator &other) const {
            return table_heap_ == other.table_heap_ && tuple_id_ == other.tuple_id_;
        }

        bool operator!=(const Iterator &other) const { return !(*this == other); }

        Iterator &operator++();

        Iterator operator++(int);

        Iterator &operator--();

        Iterator operator--(int);

        Tuple operator*();

        tuple_id_t tuple_id() const { return tuple_id_; }

      private:
        TableHeap *table_heap_;
        tuple_id_t tuple_id_;
    };

  public:
    explicit TableHeap(buffer::BufferManager *buffer_manager);

    TableHeap(buffer::BufferManager *buffer_manager, page_id_t root_page_id);

    page_id_t root_page_id() const { return root_page_id_; }

    tuple_id_t insert_tuple(const Tuple &tuple);

    bool delete_tuple(tuple_id_t tuple_id);

    std::optional<Tuple> get_tuple(tuple_id_t tuple_id);

    bool update_tuple(tuple_id_t tuple_id, const Tuple &tuple);

    Iterator begin();

    Iterator end();

  private:
    buffer::BufferManager *buffer_manager_;
    page_id_t root_page_id_;
};
}  // namespace naivedb::storage