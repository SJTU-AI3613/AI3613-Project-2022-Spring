#include "catalog/schema.h"

namespace naivedb::catalog {
column_id_t Schema::column_id(std::string_view column_name) const {
    column_id_t columns_size = columns_.size();
    for (column_id_t i = 0; i < columns_size; ++i) {
        if (columns_[i].name() == column_name) {
            return i;
        }
    }
    return INVALID_COLUMN_ID;
}
}  // namespace naivedb::catalog