#include "query/execution/execution_engine.h"

#include "query/execution/executor_builder.h"
#include "storage/tuple/tuple.h"

namespace naivedb::query {
std::vector<storage::Tuple> ExecutionEngine::execute(const PhysicalPlan *plan) {
    ExecutorBuilder builder(context_);
    auto executor = builder.build(plan);

    std::vector<storage::Tuple> result;
    std::vector<storage::Tuple> current;

    executor->init();

    while (!(current = executor->next()).empty()) {
        for (auto &&tuple : current) {
            result.emplace_back(std::move(tuple));
        }
    }

    return result;
}
}  // namespace naivedb::query