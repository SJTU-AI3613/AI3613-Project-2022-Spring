#pragma once

#include "common/types.h"
#include "common/utils.h"
#include "query/expr/expr.h"
#include "query/physical_plan/physical_plan.h"
#include "query/physical_plan/physical_plan_visitor.h"

namespace naivedb::query {
/**
 * @brief PhysicalFilter filters the tuples obtained from the child plan by checking if the predicate is satisfied.
 *
 */
class PhysicalFilter : public PhysicalPlan {
  public:
    /**
     * @brief Construct a new PhysicalFilter object
     *
     * @param output_schema
     * @param child the child plan from which tuples are obtained
     * @param predicate the predicate applied on the tuples.
     * @note predicate must not be nullptr
     */
    PhysicalFilter(const catalog::Schema *output_schema,
                   std::unique_ptr<const PhysicalPlan> &&child,
                   std::unique_ptr<const Expr> &&predicate)
        : PhysicalPlan(output_schema, make_vector(std::move(child))), predicate_(std::move(predicate)) {}

    virtual ~PhysicalFilter() = default;

    const PhysicalPlan *child() const { return child_at(0); }

    const Expr *predicate() const { return predicate_.get(); }

    virtual void accept(PhysicalPlanVisitor &visitor) const override { visitor.visit(this); }

  private:
    std::unique_ptr<const Expr> predicate_;
};
}  // namespace naivedb::query