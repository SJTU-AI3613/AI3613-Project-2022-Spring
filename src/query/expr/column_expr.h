#pragma once

#include "common/types.h"
#include "query/expr/expr.h"
#include "storage/tuple/tuple.h"
#include "type/value.h"

#include <cassert>

namespace naivedb::query {
class ColumnExpr : public Expr {
  public:
    /**
     * @brief Construct a new ColumnExpr object
     *
     * @param column_id the index of the column in the schema
     * @param return_type
     * @param left true if this column belongs to the left side of join
     */
    ColumnExpr(column_id_t column_id, type::Type return_type, bool left = true)
        : Expr({}, return_type), column_id_(column_id), left_(left) {}

    virtual ~ColumnExpr() = default;

    virtual type::Value evaluate(const storage::Tuple &tuple, const catalog::Schema *schema) const override {
        return tuple.value_at(schema, column_id_);
    }

    virtual type::Value evaluate_join(const storage::Tuple &left_tuple,
                                      const storage::Tuple &right_tuple,
                                      const catalog::Schema *left_schema,
                                      const catalog::Schema *right_schema) const override {
        return left_ ? left_tuple.value_at(left_schema, column_id_) : right_tuple.value_at(right_schema, column_id_);
    }

    virtual type::Value evaluate_aggregate(const std::vector<storage::Tuple> &tuples,
                                           const catalog::Schema *schema) const override {
        assert(!tuples.empty());
        return tuples[0].value_at(schema, column_id_);
    }

    column_id_t column_id() const { return column_id_; }

  private:
    column_id_t column_id_;
    bool left_;
};
}  // namespace naivedb::query