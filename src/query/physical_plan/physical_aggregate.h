#pragma once

#include "common/types.h"
#include "common/utils.h"
#include "query/expr/expr.h"
#include "query/physical_plan/physical_plan.h"
#include "query/physical_plan/physical_plan_visitor.h"

namespace naivedb::query {
/**
 * @brief PhysicalAggregate computes the aggregation of obtained tuples.
 *
 */
class PhysicalAggregate : public PhysicalPlan {
  public:
    /**
     * @brief Construct a new PhysicalAggregate object
     *
     * @param output_schema
     * @param child
     * @param aggregate_exprs column values or aggregation functions (e.g. MIN, MAX, SUM)
     */
    PhysicalAggregate(const catalog::Schema *output_schema,
                      std::unique_ptr<const PhysicalPlan> &&child,
                      std::vector<std::unique_ptr<const Expr>> &&aggregate_exprs)
        : PhysicalPlan(output_schema, make_vector(std::move(child))), aggregate_exprs_(std::move(aggregate_exprs)) {}

    virtual ~PhysicalAggregate() = default;

    const PhysicalPlan *child() const { return child_at(0); }

    const std::vector<std::unique_ptr<const Expr>> &aggregate_exprs() const { return aggregate_exprs_; }

    virtual void accept(PhysicalPlanVisitor &visitor) const override { visitor.visit(this); }

  private:
    std::vector<std::unique_ptr<const Expr>> aggregate_exprs_;
};
}  // namespace naivedb::query