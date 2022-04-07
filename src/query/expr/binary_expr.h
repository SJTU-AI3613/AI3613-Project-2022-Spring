#pragma once

#include "common/exception.h"
#include "common/macros.h"
#include "query/expr/expr.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value.h"

namespace naivedb::query {
// Only comparison and logical operators are supported now.
enum class BinaryOperator {
    Eq,   // =
    Ne,   // <>
    Lt,   // <
    Le,   // <=
    Gt,   // >
    Ge,   // >=
    And,  // AND
    Or,   // Or
};

class BinaryExpr : public Expr {
  public:
    /**
     * @brief Construct a new BinaryExpr object
     *
     * @param op the binary operator
     * @param left the left child expression
     * @param right the right child expression
     */
    BinaryExpr(BinaryOperator op, std::unique_ptr<const Expr> &&left, std::unique_ptr<const Expr> &&right);

    virtual ~BinaryExpr() = default;

    virtual type::Value evaluate(const storage::Tuple &tuple, const catalog::Schema *schema) const override;

    virtual type::Value evaluate_join(const storage::Tuple &left_tuple,
                                      const storage::Tuple &right_tuple,
                                      const catalog::Schema *left_schema,
                                      const catalog::Schema *right_schema) const override;

    virtual type::Value evaluate_aggregate(const std::vector<storage::Tuple> &tuples,
                                           const catalog::Schema *schema) const override {
        UNREACHABLE;
        return type::Value();
    }

    const Expr *left_expr() const { return child_at(0); }

    const Expr *right_expr() const { return child_at(1); }

  private:
    BinaryOperator op_;
};
}  // namespace naivedb::query