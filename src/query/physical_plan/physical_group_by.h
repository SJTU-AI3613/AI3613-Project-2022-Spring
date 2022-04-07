#pragma once

#include "common/types.h"
#include "common/utils.h"
#include "query/physical_plan/physical_plan.h"
#include "query/physical_plan/physical_plan_visitor.h"

namespace naivedb::query {
/**
 * @brief PhysicalGroupBy partitions the obtained tuples into buckets based on column values.
 * @note Only single group-by column is supported for now.
 *
 */
class PhysicalGroupBy : public PhysicalPlan {
  public:
    /**
     * @brief Construct a new PhysicalGroupBy object
     *
     * @param output_schema
     * @param child
     * @param column_id
     */
    PhysicalGroupBy(const catalog::Schema *output_schema,
                    std::unique_ptr<const PhysicalPlan> &&child,
                    column_id_t column_id)
        : PhysicalPlan(output_schema, make_vector(std::move(child))), column_id_(column_id) {}

    virtual ~PhysicalGroupBy() = default;

    const PhysicalPlan *child() const { return child_at(0); }

    column_id_t column_id() const { return column_id_; }

    virtual void accept(PhysicalPlanVisitor &visitor) const override { visitor.visit(this); }

  private:
    column_id_t column_id_;
};
}  // namespace naivedb::query