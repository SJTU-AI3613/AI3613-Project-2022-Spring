#pragma once

#include "common/types.h"
#include "common/utils.h"
#include "query/expr/expr.h"
#include "query/physical_plan/physical_plan.h"
#include "query/physical_plan/physical_plan_visitor.h"

namespace naivedb::query {
/**
 * @brief PhysicalUpdate updates the tuples satisfying the predicate.
 *
 */
class PhysicalUpdate : public PhysicalPlan {
  public:
    /**
     * @brief Construct a new PhysicalUpdate object
     *
     * @param update_columns pairs of column id and the new value
     * @param table_id the identifier of the table to be updated
     * @param predicate the predicate applied on the tuples.
     * @note predicate can be nullptr
     */
    PhysicalUpdate(std::vector<std::pair<column_id_t, type::Value>> &&update_columns,
                   table_id_t table_id,
                   std::unique_ptr<const Expr> &&predicate)
        : PhysicalPlan(nullptr, {})
        , update_columns_(std::move(update_columns))
        , table_id_(table_id)
        , predicate_(std::move(predicate)) {}

    virtual ~PhysicalUpdate() = default;

    const std::vector<std::pair<column_id_t, type::Value>> &update_columns() const { return update_columns_; }

    table_id_t table_id() const { return table_id_; }

    const Expr *predicate() const { return predicate_.get(); }

    virtual void accept(PhysicalPlanVisitor &visitor) const override { visitor.visit(this); }

  private:
    std::vector<std::pair<column_id_t, type::Value>> update_columns_;
    table_id_t table_id_;
    std::unique_ptr<const Expr> predicate_;
};
}  // namespace naivedb::query