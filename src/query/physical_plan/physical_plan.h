#pragma once

#include <memory>
#include <vector>

namespace naivedb {
namespace query {
class PhysicalPlanVisitor;
}
namespace catalog {
class Schema;
}
}  // namespace naivedb

namespace naivedb::query {
/**
 * @brief PhysicalPlan represents all possible types of physical plan nodes in our system. Plan nodes are modeled as
 * trees, so each plan node can have a variable number of children. Per the Volcano model, the plan node receives the
 * tuples of its children. The ordering of the children may matter.
 *
 */
class PhysicalPlan {
  public:
    /**
     * @brief Construct a new PhysicalPlan object
     *
     * @param output_schema the schema for the output of this plan node
     * @param children the children of this plan node
     */
    PhysicalPlan(const catalog::Schema *output_schema, std::vector<std::unique_ptr<const PhysicalPlan>> &&children)
        : output_schema_(output_schema), children_(std::move(children)) {}

    /**
     * @brief Destroy the PhysicalPlan object
     *
     */
    virtual ~PhysicalPlan() = default;

    /**
     * @brief Get the schema for the output of this plan node
     *
     * @return const catalog::Schema*
     */
    const catalog::Schema *output_schema() const { return output_schema_; }

    /**
     * @brief Get the child of this plan node at index child_id
     *
     * @param child_id
     * @return const PhysicalPlan*
     */
    const PhysicalPlan *child_at(size_t child_id) const { return children_[child_id].get(); }

    /**
     * @brief Get all the children of this plan node
     *
     * @return const std::vector<std::unique_ptr<const PhysicalPlan>>&
     */
    const std::vector<std::unique_ptr<const PhysicalPlan>> &children() const { return children_; }

    /**
     * @brief Accept the plan visitor
     *
     * @param visitor
     */
    virtual void accept(PhysicalPlanVisitor &visitor) const = 0;

  private:
    const catalog::Schema *output_schema_;
    std::vector<std::unique_ptr<const PhysicalPlan>> children_;
};
}  // namespace naivedb::query