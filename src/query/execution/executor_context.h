#pragma once

namespace naivedb {
namespace catalog {
class Catalog;
}
namespace buffer {
class BufferManager;
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
/**
 * @brief ExecutorContext stores all the context necessary to run an executor.
 *
 */
class ExecutorContext {
  public:
    ExecutorContext(buffer::BufferManager *buffer_manager,
                    catalog::Catalog *catalog,
                    transaction::Transaction *transaction,
                    lock::LockManager *lock_manager,
                    log::LogManager *log_manager)
        : buffer_manager_(buffer_manager)
        , catalog_(catalog)
        , transaction_(transaction)
        , lock_manager_(lock_manager)
        , log_manager_(log_manager) {}

    buffer::BufferManager *buffer_manager() { return buffer_manager_; }

    catalog::Catalog *catalog() { return catalog_; }

    transaction::Transaction *transaction() { return transaction_; }

    lock::LockManager *lock_manager() { return lock_manager_; }

    log::LogManager *log_manager() { return log_manager_; }

  private:
    buffer::BufferManager *buffer_manager_;
    catalog::Catalog *catalog_;
    transaction::Transaction *transaction_;
    lock::LockManager *lock_manager_;
    log::LogManager *log_manager_;
};
}  // namespace naivedb::query