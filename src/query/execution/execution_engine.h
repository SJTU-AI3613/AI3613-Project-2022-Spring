#pragma once

#include "common/macros.h"
#include "query/execution/executor_context.h"

#include <vector>

namespace naivedb {
namespace buffer {
class BufferManager;
}
namespace catalog {
class Catalog;
}
namespace query {
class PhysicalPlan;
}
namespace storage {
class Tuple;
}
}  // namespace naivedb

namespace naivedb::query {
class ExecutionEngine {
    DISALLOW_COPY_AND_MOVE(ExecutionEngine)

  public:
    ExecutionEngine(buffer::BufferManager *buffer_manager, catalog::Catalog *catalog)
        : context_(buffer_manager, catalog) {}

    std::vector<storage::Tuple> execute(const PhysicalPlan *plan);

  private:
    ExecutorContext context_;
};
}  // namespace naivedb::query