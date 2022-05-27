#pragma once

#include "common/exception.h"
#include "lock/lock_manager.h"
#include "transaction/transaction.h"

#include <chrono>
#include <thread>

namespace naivedb {
class TestLockManager : public lock::LockManager {
  public:
    TestLockManager() : lock::LockManager(false) {}

    TestLockManager &add_shared_lock(tuple_id_t tuple_id, txn_id_t txn_id) {
        lock_table_[tuple_id].shared_locks_.emplace(txn_id);
        return *this;
    }

    TestLockManager &add_exclusive_lock(tuple_id_t tuple_id, txn_id_t txn_id) {
        lock_table_[tuple_id].exclusive_lock_ = txn_id;
        return *this;
    }

    TestLockManager &add_wait(tuple_id_t tuple_id, txn_id_t txn_id, LockMode mode) {
        lock_table_[tuple_id].wait_list_[txn_id] = mode;
        return *this;
    }
};

#define ABORT_IF(condition)                                                                                            \
    do {                                                                                                               \
        if (condition) {                                                                                               \
            throw TransactionAbortException(txn_id);                                                                   \
        }                                                                                                              \
    } while (0)

inline void sleep_for(size_t ms) { std::this_thread::sleep_for(std::chrono::duration<size_t, std::milli>(ms)); }
}  // namespace naivedb