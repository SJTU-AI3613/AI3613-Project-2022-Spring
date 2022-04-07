#pragma once

#include "common/utils.h"
#include "query/expr/expr.h"
#include "query/physical_plan/physical_plan.h"
#include "query/physical_plan/physical_plan_visitor.h"

namespace naivedb::query {
/**
 * @brief Hash join performs a JOIN operation with a hash table
 *
 */
class PhysicalHashJoin : public PhysicalPlan {
  public:
    /**
     * @brief Construct a new PhysicalHashJoin object
     *
     * @param output_schema
     * @param children two sequential scan children plans
     * @param left_key_expr the expression for the left JOIN key
     * @param right_key_expr the expression for the right JOIN key
     */
    PhysicalHashJoin(const catalog::Schema *output_schema,
                     std::unique_ptr<const PhysicalPlan> &&left,
                     std::unique_ptr<const PhysicalPlan> &&right,
                     std::unique_ptr<const Expr> &&left_key_expr,
                     std::unique_ptr<const Expr> &&right_key_expr)
        : PhysicalPlan(output_schema, make_vector(std::move(left), std::move(right)))
        , left_key_expr_(std::move(left_key_expr))
        , right_key_expr_(std::move(right_key_expr)) {}

    virtual ~PhysicalHashJoin() = default;

    const Expr *left_key_expr() const { return left_key_expr_.get(); }

    const Expr *right_key_expr() const { return right_key_expr_.get(); }

    const PhysicalPlan *left_plan() const { return child_at(0); }

    const PhysicalPlan *right_plan() const { return child_at(1); }

    virtual void accept(PhysicalPlanVisitor &visitor) const override { visitor.visit(this); }

  private:
    std::unique_ptr<const Expr> left_key_expr_;
    std::unique_ptr<const Expr> right_key_expr_;
};
}  // namespace naivedb::query