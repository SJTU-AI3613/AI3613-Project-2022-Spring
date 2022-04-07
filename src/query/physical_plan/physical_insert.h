#pragma once

#include "common/types.h"
#include "common/utils.h"
#include "query/physical_plan/physical_plan.h"
#include "query/physical_plan/physical_plan_visitor.h"
#include "type/value.h"

namespace naivedb::query {
/**
 * @brief PhysicalInsert identifies a table into which tuples are inserted.
 * @note Only raw insert is supported for now.
 *
 */
class PhysicalInsert : public PhysicalPlan {
  public:
    /**
     * @brief Construct a new PhysicalInsert object
     *
     * @param raw_values the raw values to be inserted
     * @param table_id the identifier of the table to be inserted into
     */
    PhysicalInsert(std::vector<std::vector<type::Value>> &&raw_values, table_id_t table_id)
        : PhysicalPlan(nullptr, {}), raw_values_(std::move(raw_values)), table_id_(table_id) {}

    virtual ~PhysicalInsert() = default;

    const std::vector<std::vector<type::Value>> &raw_values() const { return raw_values_; }

    table_id_t table_id() const { return table_id_; }

    virtual void accept(PhysicalPlanVisitor &visitor) const override { visitor.visit(this); }

  private:
    std::vector<std::vector<type::Value>> raw_values_;
    table_id_t table_id_;
};
}  // namespace naivedb::query