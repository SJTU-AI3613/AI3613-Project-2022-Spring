#include "storage/tuple/tuple.h"

#include "catalog/schema.h"
#include "type/value.h"

namespace naivedb::storage {
Tuple::Tuple(const std::vector<type::Value> &values) {
    size_t total_size = 0;
    for (auto &val : values) {
        total_size += val.size();
    }

    data_.resize(total_size);
    size_t offset = 0;

    for (auto &val : values) {
        val.serialize(data_.data() + offset);
        offset += val.size();
    }
}

type::Value Tuple::value_at(const catalog::Schema *schema, column_id_t column_id) const {
    auto &column = schema->column(column_id);
    auto offset = schema->column_offset(column_id);
    return type::Value::deserialize(data_.data() + offset, column.type());
}

void Tuple::set_value_at(const catalog::Schema *schema, column_id_t column_id, const type::Value &value) {
    auto offset = schema->column_offset(column_id);
    value.serialize(data_.data() + offset);
}

std::vector<type::Value> Tuple::values(const catalog::Schema *schema) const {
    std::vector<type::Value> values;
    auto column_count = schema->columns().size();
    for (size_t column_id = 0; column_id < column_count; ++column_id) {
        auto offset = schema->column_offset(column_id);
        auto type = schema->column(column_id).type();
        values.emplace_back(type::Value::deserialize(data_.data() + offset, type));
    }
    return values;
}
}  // namespace naivedb::storage