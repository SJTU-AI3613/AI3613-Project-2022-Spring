#pragma once

#include "common/utils.h"
#include "query/expr/expr.h"

namespace naivedb::query {
enum class AggregateOperator {
    Min,
    Max,
    Sum,
};

class AggregateExpr : public Expr {
  public:
    /**
     * @brief Construct a new AggregateExpr object
     *
     * @param op the aggregation function
     * @param return_type
     * @param child
     */
    AggregateExpr(AggregateOperator op, type::Type return_type, std::unique_ptr<const Expr> &&child)
        : Expr(make_vector(std::move(child)), return_type), op_(op) {}

    virtual ~AggregateExpr() = default;

    virtual type::Value evaluate(const storage::Tuple &tuple, const catalog::Schema *schema) const override;

    virtual type::Value evaluate_join(const storage::Tuple &left_tuple,
                                      const storage::Tuple &right_tuple,
                                      const catalog::Schema *left_schema,
                                      const catalog::Schema *right_schema) const override;

    virtual type::Value evaluate_aggregate(const std::vector<storage::Tuple> &tuples,
                                           const catalog::Schema *schema) const override;

    const Expr *child() const { return child_at(0); }

  private:
    AggregateOperator op_;
};
}  // namespace naivedb::query