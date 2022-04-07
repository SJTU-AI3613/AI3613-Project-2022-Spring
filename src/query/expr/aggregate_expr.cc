#include "query/expr/aggregate_expr.h"

#include "common/macros.h"
#include "storage/tuple/tuple.h"
#include "type/value.h"

#include <cassert>
#include <functional>
#include <numeric>

namespace naivedb::query {
type::Value AggregateExpr::evaluate(const storage::Tuple &tuple, const catalog::Schema *schema) const {
    UNREACHABLE;
    return type::Value();
}

type::Value AggregateExpr::evaluate_join(const storage::Tuple &left_tuple,
                                         const storage::Tuple &right_tuple,
                                         const catalog::Schema *left_schema,
                                         const catalog::Schema *right_schema) const {
    UNREACHABLE;
    return type::Value();
}

type::Value AggregateExpr::evaluate_aggregate(const std::vector<storage::Tuple> &tuples,
                                              const catalog::Schema *schema) const {
    assert(!tuples.empty());

    std::vector<type::Value> values;
    for (auto &tuple : tuples) {
        values.emplace_back(child()->evaluate(tuple, schema));
    }

    type::Value result;
    std::function<void(type::Value &, const type::Value &)> aggregate_f;

    switch (op_) {
        case AggregateOperator::Min:
            result = values[0];
            aggregate_f = [](auto &aggr, auto &value) { aggr = aggr.min(value); };
            break;

        case AggregateOperator::Max:
            result = values[0];
            aggregate_f = [](auto &aggr, auto &value) { aggr = aggr.max(value); };
            break;

        case AggregateOperator::Sum:
            result = type::Value::get_default(child()->return_type());
            aggregate_f = [](auto &aggr, auto &value) { aggr = aggr.add(value); };
            break;
    }

    for (auto &value : values) {
        aggregate_f(result, value);
    }

    return result;
}
}  // namespace naivedb::query