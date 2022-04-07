#pragma once

#include "query/expr/expr.h"
#include "type/value.h"

namespace naivedb::query {
class ConstExpr : public Expr {
  public:
    /**
     * @brief Construct a new ConstExpr object
     *
     * @param value
     */
    explicit ConstExpr(type::Value &&value) : Expr({}, value.type()), value_(std::move(value)) {}

    virtual ~ConstExpr() = default;

    virtual type::Value evaluate(const storage::Tuple &tuple, const catalog::Schema *schema) const override {
        return value_;
    }

    virtual type::Value evaluate_join(const storage::Tuple &left_tuple,
                                      const storage::Tuple &right_tuple,
                                      const catalog::Schema *left_schema,
                                      const catalog::Schema *right_schema) const override {
        return value_;
    }

    virtual type::Value evaluate_aggregate(const std::vector<storage::Tuple> &tuples,
                                           const catalog::Schema *schema) const override {
        return value_;
    }

  private:
    type::Value value_;
};
}  // namespace naivedb::query