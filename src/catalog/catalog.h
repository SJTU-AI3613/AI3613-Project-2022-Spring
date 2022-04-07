#pragma once

#include "catalog/schema.h"
#include "common/format.h"
#include "common/macros.h"
#include "common/types.h"

#include <list>
#include <memory>
#include <unordered_map>
#include <vector>

namespace naivedb {
namespace buffer {
class BufferManager;
}
namespace catalog {
class TableInfo;
}  // namespace catalog
}  // namespace naivedb

namespace naivedb::catalog {
class Catalog {
    DISALLOW_COPY_AND_MOVE(Catalog)

    struct InnerTableInfo {
        std::string name_;
        std::unique_ptr<Schema> schema_;
        page_id_t root_page_id_;

        InnerTableInfo(std::string_view name, std::unique_ptr<Schema> &&schema, page_id_t root_page_id)
            : name_(name), schema_(std::move(schema)), root_page_id_(root_page_id) {}
    };

  public:
    Catalog(buffer::BufferManager *buffer_manager);

    table_id_t get_table_id(std::string_view table_name) const;

    TableInfo get_table_info(table_id_t table_id) const;

    table_id_t create_table(std::string_view table_name, Schema &&schema);

    void drop_table(table_id_t table_id);

  private:
    buffer::BufferManager *buffer_manager_;
    std::unordered_map<std::string_view, table_id_t> table_index_;
    std::vector<InnerTableInfo> table_info_;
    std::list<table_id_t> free_slots_;
};
}  // namespace naivedb::catalog