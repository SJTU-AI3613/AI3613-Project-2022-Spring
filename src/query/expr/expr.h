#pragma once

#include "type/type.h"

#include <vector>

namespace naivedb {
namespace catalog {
class Schema;
}
namespace storage {
class Tuple;
}
namespace type {
class Value;
}
}  // namespace naivedb

namespace naivedb::query {
/**
 * @brief Expr represents all possible types of expressions in our system. Expressions are modeled as trees, i.e. every
 * expression may havea variable number of children.
 *
 */
class Expr {
  public:
    /**
     * @brief Construct a new Expr object
     *
     * @param children the children of this expression
     * @param return_type the return type of this expression when it is evaluated
     */
    Expr(std::vector<std::unique_ptr<const Expr>> &&children, type::Type return_type)
        : children_(std::move(children)), return_type_(return_type) {}

    /**
     * @brief Destroy the Expr object
     *
     */
    virtual ~Expr() = default;

    /**
     * @brief Evaluate the expression with the given tuple and schema
     *
     * @param tuple
     * @param schema
     * @return type::Value
     */
    virtual type::Value evaluate(const storage::Tuple &tuple, const catalog::Schema *schema) const = 0;

    /**
     * @brief Evaluate the expression with the tuples and schemas of the two tables to be joined.
     *
     * @param left_tuple
     * @param right_tuple
     * @param left_schema
     * @param right_schema
     * @return type::Value
     */
    virtual type::Value evaluate_join(const storage::Tuple &left_tuple,
                                      const storage::Tuple &right_tuple,
                                      const catalog::Schema *left_schema,
                                      const catalog::Schema *right_schema) const = 0;

    /**
     * @brief Evaluate the expression with the grouped tuples and return an aggregation
     *
     * @param tuples
     * @param schema
     * @return type::Value
     */
    virtual type::Value evaluate_aggregate(const std::vector<storage::Tuple> &tuples,
                                           const catalog::Schema *schema) const = 0;

    /**
     * @brief Get the child of this expression at index child_id
     *
     * @param child_id
     * @return const Expr*
     */
    const Expr *child_at(size_t child_id) const { return children_[child_id].get(); }

    /**
     * @brief Get the children of this expression
     *
     * @return const std::vector<std::unique_ptr<const Expr>>&
     */
    const std::vector<std::unique_ptr<const Expr>> &children() const { return children_; }

    /**
     * @brief Get the return type of this expression
     *
     * @return type::Type
     */
    type::Type return_type() const { return return_type_; }

  private:
    std::vector<std::unique_ptr<const Expr>> children_;
    type::Type return_type_;
};
}  // namespace naivedb::query