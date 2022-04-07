#pragma once

#include "common/utils.h"
#include "query/expr/expr.h"
#include "query/physical_plan/physical_plan.h"
#include "query/physical_plan/physical_plan_visitor.h"

namespace naivedb::query {
/**
 * @brief PhysicalNestedLoopJoin joins tuples from two child plan nodes.
 *
 */
class PhysicalNestedLoopJoin : public PhysicalPlan {
  public:
    /**
     * @brief Construct a new PhysicalNestedLoopJoin object
     *
     * @param output_schema
     * @param children two sequential scan children plans
     * @param predicate the predicate to join with. tuple A and B are joined if predicate(A, B) == true or predicate ==
     * nullptr.
     * @note predicate can be nullptr
     */
    PhysicalNestedLoopJoin(const catalog::Schema *output_schema,
                           std::unique_ptr<const PhysicalPlan> &&left,
                           std::unique_ptr<const PhysicalPlan> &&right,
                           std::unique_ptr<const Expr> &&predicate)
        : PhysicalPlan(output_schema, make_vector(std::move(left), std::move(right)))
        , predicate_(std::move(predicate)) {}

    virtual ~PhysicalNestedLoopJoin() = default;

    const Expr *predicate() const { return predicate_.get(); }

    const PhysicalPlan *left_plan() const { return child_at(0); }

    const PhysicalPlan *right_plan() const { return child_at(1); };

    virtual void accept(PhysicalPlanVisitor &visitor) const override { visitor.visit(this); }

  private:
    std::unique_ptr<const Expr> predicate_;
};
}  // namespace naivedb::query