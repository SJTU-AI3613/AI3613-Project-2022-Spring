#include "query/expr/binary_expr.h"

#include "common/macros.h"
#include "common/utils.h"
#include "type/type.h"
#include "type/type_id.h"

namespace naivedb::query {
BinaryExpr::BinaryExpr(BinaryOperator op, std::unique_ptr<const Expr> &&left, std::unique_ptr<const Expr> &&right)
    : Expr(make_vector(std::move(left), std::move(right)), type::Type(type::Boolean())), op_(op) {}

type::Value BinaryExpr::evaluate(const storage::Tuple &tuple, const catalog::Schema *schema) const {
    auto lhs = left_expr()->evaluate(tuple, schema);
    auto rhs = right_expr()->evaluate(tuple, schema);
    switch (op_) {
        case BinaryOperator::Eq:
            return lhs.eq(rhs);

        case BinaryOperator::Ne:
            return lhs.ne(rhs);

        case BinaryOperator::Lt:
            return lhs.lt(rhs);

        case BinaryOperator::Le:
            return lhs.le(rhs);

        case BinaryOperator::Gt:
            return lhs.gt(rhs);

        case BinaryOperator::Ge:
            return lhs.ge(rhs);

        case BinaryOperator::And:
            return lhs.land(rhs);

        case BinaryOperator::Or:
            return lhs.lor(rhs);
    }

    UNREACHABLE;
    return type::Value();
}

type::Value BinaryExpr::evaluate_join(const storage::Tuple &left_tuple,
                                      const storage::Tuple &right_tuple,
                                      const catalog::Schema *left_schema,
                                      const catalog::Schema *right_schema) const {
    auto lhs = left_expr()->evaluate_join(left_tuple, right_tuple, left_schema, right_schema);
    auto rhs = right_expr()->evaluate_join(left_tuple, right_tuple, left_schema, right_schema);
    switch (op_) {
        case BinaryOperator::Eq:
            return lhs.eq(rhs);

        case BinaryOperator::Ne:
            return lhs.ne(rhs);

        case BinaryOperator::Lt:
            return lhs.lt(rhs);

        case BinaryOperator::Le:
            return lhs.le(rhs);

        case BinaryOperator::Gt:
            return lhs.gt(rhs);

        case BinaryOperator::Ge:
            return lhs.ge(rhs);

        case BinaryOperator::And:
            return lhs.land(rhs);

        case BinaryOperator::Or:
            return lhs.lor(rhs);
    }

    UNREACHABLE;
    return type::Value();
}
}  // namespace naivedb::query