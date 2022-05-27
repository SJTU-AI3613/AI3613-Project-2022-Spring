#include "lock/lock_manager.h"

#include "common/constants.h"
#include "common/graph.h"
#include "common/macros.h"
#include "common/types.h"
#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"

#include <mutex>
#include <queue>
#include <thread>
#include <unordered_set>

namespace naivedb::lock {
bool LockManager::lock_shared(transaction::Transaction *txn, tuple_id_t tuple_id) {
    // TODO(Project-2 Part-B): Insert statements that add txn to the wait list and remove it from the wait list.

    // hold the latch
    std::unique_lock latch(latch_);
    // return false immediately if txn is not in growing phase
    if (txn->state() != transaction::TransactionState::Growing) {
        return false;
    }
    auto &lock_list = lock_table_[tuple_id];
    auto txn_id = txn->transaction_id();
    // check double lock
    if (lock_list.shared_locks_.find(txn_id) != lock_list.shared_locks_.end()) {
        return false;
    }
    // if the exclusive lock is held by another transaction, block this thread
    if (lock_list.exclusive_lock_ != INVALID_TXN_ID) {
        // cv_.wait() must be put in the loop body since the condition may change before it holds the latch again.
        while (lock_list.exclusive_lock_ != INVALID_TXN_ID) {
            // release the latch and block the thread
            lock_list.cv_.wait(latch);
            // now the thread is awakened and the latch is held
            // return false if the transaction is aborted
            if (txn->state() == transaction::TransactionState::Aborted) {
                return false;
            }
        }
    }
    // now we can hold the shared lock
    lock_list.shared_locks_.emplace(txn_id);
    // add tuple_id to the txn's shared lock set
    txn->shared_lock_set().emplace(tuple_id);
    return true;
}

bool LockManager::lock_exclusive(transaction::Transaction *txn, tuple_id_t tuple_id) {
    // TODO(Project-2 Part-A, B): Implement this method
    UNIMPLEMENTED;
    return true;
}

bool LockManager::lock_convert(transaction::Transaction *txn, tuple_id_t tuple_id) {
    // TODO(Project-2 Part-A, B): Implement this method
    UNIMPLEMENTED;
    return true;
}

bool LockManager::unlock(transaction::Transaction *txn, tuple_id_t tuple_id) {
    // TODO(Project-2 Part-A): Implement this method
    UNIMPLEMENTED;
    return true;
}

Graph<txn_id_t> LockManager::build_graph() const {
    // TODO(Project-2 Part-B): Implement this method
    // build the wait-for graph according to the lock table
    // no need to hold the latch
    UNIMPLEMENTED;
    return Graph<txn_id_t>();
}

txn_id_t LockManager::has_cycle(const Graph<txn_id_t> &graph) const {
    // TODO(Project-2 Part-B): Implement this method
    // use any algorithm you like
    // no need to hold the latch
    UNIMPLEMENTED;
    return 0;
}

void LockManager::deadlock_detection() {
    while (!stop_deadlock_detector_.load()) {
        std::unique_lock latch(latch_);
        // TODO(Project-2 Part-B): Implement this method
        // 1. build the wait-for graph
        // 2. check whether there is a cycle in the graph
        // 3. if a cycle is found, get the victim transaction
        // 4. set the state of the victim transaction to aborted and wake it up

        // unlock the latch before sleeping
        latch.unlock();
        std::this_thread::sleep_for(std::chrono::duration<size_t, std::milli>(DEADLOCK_DETECTION_INTERVAL));
    }
}
}  // namespace naivedb::lock