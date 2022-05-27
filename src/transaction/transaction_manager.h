#pragma once

#include "common/macros.h"
#include "common/types.h"

#include <atomic>

namespace naivedb {
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

namespace naivedb::transaction {
/**
 * @brief TransactionManager manages all transactions in the system.
 *
 */
class TransactionManager {
    DISALLOW_COPY_AND_MOVE(TransactionManager)
  public:
    TransactionManager(lock::LockManager *lock_manager,
                       log::LogManager *log_manager = nullptr,
                       buffer::BufferManager *buffer_manager = nullptr)
        : next_txn_id_(0), lock_manager_(lock_manager), log_manager_(log_manager), buffer_manager_(buffer_manager) {}

    /**
     * @brief Destroy the TransactionManager object. Clear global_txn_map.
     *
     */
    ~TransactionManager();

    /**
     * @brief Begin a new transaction and return its pointer.
     *
     * @return Transaction*
     */
    Transaction *begin_transaction();

    /**
     * @brief Abort a transaction.
     *
     * @param txn_id
     */
    void abort_transaction(txn_id_t txn_id);

    /**
     * @brief Commit a transaction.
     *
     * @param txn_id
     */
    void commit_transaction(txn_id_t txn_id);

    /**
     * @brief Get the transaction with txn_id. Return nullptr if the transaction does not exist.
     *
     * @param txn_id
     * @return Transaction*
     */
    static Transaction *get_transaction(txn_id_t txn_id);

  private:
    /**
     * @brief Rollback the transaction by undo log
     *
     */
    void rollback(Transaction *txn);

    /**
     * @brief Release all the locks of the transaction.
     *
     */
    void release_all_locks(Transaction *txn);

  private:
    std::atomic<txn_id_t> next_txn_id_;

    lock::LockManager *lock_manager_;
    log::LogManager *log_manager_;
    buffer::BufferManager *buffer_manager_;
};
}  // namespace naivedb::transaction