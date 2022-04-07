#pragma once

#include "common/utils.h"
#include "query/expr/expr.h"
#include "query/physical_plan/physical_plan.h"
#include "query/physical_plan/physical_plan_visitor.h"

#include <vector>

namespace naivedb::query {
/**
 * @brief PhysicalProjection represents a projection operation.
 *
 */
class PhysicalProjection : public PhysicalPlan {
  public:
    /**
     * @brief Construct a new PhysicalProjection object
     *
     * @param output_schema
     * @param child the child plan from which tuples are obtained
     * @param project_exprs the expressions used to project columns
     */
    PhysicalProjection(const catalog::Schema *output_schema,
                       std::unique_ptr<const PhysicalPlan> &&child,
                       std::vector<std::unique_ptr<const Expr>> &&project_exprs)
        : PhysicalPlan(output_schema, make_vector(std::move(child))), project_exprs_(std::move(project_exprs)) {}

    virtual ~PhysicalProjection() = default;

    const std::vector<std::unique_ptr<const Expr>> &project_exprs() const { return project_exprs_; }

    const PhysicalPlan *child() const { return child_at(0); }

    virtual void accept(PhysicalPlanVisitor &visitor) const override { visitor.visit(this); }

  private:
    std::vector<std::unique_ptr<const Expr>> project_exprs_;
};
}  // namespace naivedb::query