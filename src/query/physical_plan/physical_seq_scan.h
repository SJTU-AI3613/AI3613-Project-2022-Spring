#pragma once

#include "common/types.h"
#include "query/expr/expr.h"
#include "query/physical_plan/physical_plan.h"
#include "query/physical_plan/physical_plan_visitor.h"

namespace naivedb::query {
/**
 * @brief PhysicalSeqScan represents a sequential table scan operation.
 *
 */
class PhysicalSeqScan : public PhysicalPlan {
  public:
    /**
     * @brief Construct a new PhysicalSeqScan object
     *
     * @param output_schema
     * @param table_id the identifier of the table to be scanned
     */
    PhysicalSeqScan(const catalog::Schema *output_schema, table_id_t table_id)
        : PhysicalPlan(output_schema, {}), table_id_(table_id) {}

    virtual ~PhysicalSeqScan() = default;

    table_id_t table_id() const { return table_id_; }

    virtual void accept(PhysicalPlanVisitor &visitor) const override { visitor.visit(this); }

  private:
    table_id_t table_id_;
};
}  // namespace naivedb::query