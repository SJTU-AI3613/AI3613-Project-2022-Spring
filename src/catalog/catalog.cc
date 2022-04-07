#include "catalog/catalog.h"

#include "catalog/schema.h"
#include "catalog/table_info.h"
#include "common/constants.h"
#include "storage/table/table_heap.h"

#include <string_view>

namespace naivedb::catalog {
Catalog::Catalog(buffer::BufferManager *buffer_manager) : buffer_manager_(buffer_manager) {}

table_id_t Catalog::get_table_id(std::string_view table_name) const {
    auto iter = table_index_.find(table_name);
    if (iter == table_index_.end()) {
        return INVALID_TABLE_ID;
    }
    return iter->second;
}

TableInfo Catalog::get_table_info(table_id_t table_id) const {
    return TableInfo(table_id,
                     table_info_[table_id].name_,
                     table_info_[table_id].schema_.get(),
                     table_info_[table_id].root_page_id_);
}

table_id_t Catalog::create_table(std::string_view table_name, Schema &&schema) {
    if (table_index_.find(table_name) != table_index_.end()) {
        return INVALID_TABLE_ID;
    }
    storage::TableHeap table_heap(buffer_manager_);
    table_id_t table_id;
    if (!free_slots_.empty()) {
        table_id = free_slots_.front();
        free_slots_.pop_front();
        table_info_[table_id] = InnerTableInfo(
            std::string(table_name), std::make_unique<Schema>(std::move(schema)), table_heap.root_page_id());
    } else {
        table_id = table_info_.size();
        table_info_.emplace_back(
            std::string(table_name), std::make_unique<Schema>(std::move(schema)), table_heap.root_page_id());
    }
    table_index_[table_name] = table_id;
    return table_id;
}

void Catalog::drop_table(table_id_t table_id) {
    auto &table_info = table_info_[table_id];
    table_index_.erase(table_info.name_);
    free_slots_.emplace_back(table_id);
}
}  // namespace naivedb::catalog