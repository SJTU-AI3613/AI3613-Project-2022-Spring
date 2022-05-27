#pragma once

#include "common/macros.h"
#include "query/execution/executor_context.h"

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
namespace transaction {
class Transaction;
}
namespace lock {
class LockManager;
}
namespace log {
class LogManager;
}
}  // namespace naivedb

namespace naivedb::query {
class ExecutionEngine {
    DISALLOW_COPY_AND_MOVE(ExecutionEngine)

  public:
    ExecutionEngine(buffer::BufferManager *buffer_manager,
                    catalog::Catalog *catalog,
                    transaction::Transaction *transaction = nullptr,
                    lock::LockManager *lock_manager = nullptr,
                    log::LogManager *log_manager = nullptr)
        : context_(buffer_manager, catalog, transaction, lock_manager, log_manager) {}

    std::vector<storage::Tuple> execute(const PhysicalPlan *plan);

  private:
    ExecutorContext context_;
};
}  // namespace naivedb::query