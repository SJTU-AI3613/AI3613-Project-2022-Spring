#pragma once

#include "common/constants.h"
#include "common/graph.h"
#include "common/macros.h"
#include "common/types.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace naivedb {
namespace transaction {
class Transaction;
}
}  // namespace naivedb

namespace naivedb::lock {
/**
 * @brief LockManager manages all locks in the system and handles deadlock.
 *
 */
class LockManager {
    DISALLOW_COPY_AND_MOVE(LockManager)

  public:
    enum class LockMode {
        Shared,
        Exclusive,
    };

  private:
    struct LockList {
        LockList() : exclusive_lock_(INVALID_TXN_ID), wait_conversion_(false) {}

        // granted shared locks and exclusive lock
        std::unordered_set<txn_id_t> shared_locks_;
        txn_id_t exclusive_lock_;

        // transactions waiting on this tuple
        // when a transaction is waiting for lock conversion, use Exclusive as its lock mode
        std::unordered_map<txn_id_t, LockMode> wait_list_;

        // whether there is a transaction waiting for lock conversion
        bool wait_conversion_;

        // for notifying blocked transactions on this tuple.
        std::condition_variable cv_;
    };

  public:
    explicit LockManager(bool enable_deadlock_detection) : enable_deadlock_detection_(enable_deadlock_detection) {
        if (enable_deadlock_detection_) {
            stop_deadlock_detector_.store(false);
            deadlock_detector_ = std::thread([this]() { deadlock_detection(); });
        }
    }

    ~LockManager() {
        if (enable_deadlock_detection_) {
            stop_deadlock_detector_.store(true);
            deadlock_detector_.join();
        }
    }

    /**
     * @brief Acquire a shared lock on tuple_id.
     * @note The current thread should be blocked on waiting.
     *
     * @param txn the transaction requesting the shared lock
     * @param tuple_id
     * @return true if the lock is granted
     * @return false if:
     * 1) the transaction is not in growing phase
     * 2) the transaction locks the tuple twice
     * 3) the transaction is aborted after awakened
     */
    bool lock_shared(transaction::Transaction *txn, tuple_id_t tuple_id);

    /**
     * @brief Acquire an exclusive lock on tuple_id.
     * @note The current thread should be blocked on waiting.
     *
     * @param txn the transaction requesting the exclusive lock
     * @param tuple_id
     * @return true if the lock is granted
     * @return false if:
     * 1) the transaction is not in growing phase
     * 2) the transaction locks the tuple twice
     * 3) the transaction is aborted after awakened
     */
    bool lock_exclusive(transaction::Transaction *txn, tuple_id_t tuple_id);

    /**
     * @brief Convert a shared lock to an exclusive lock on tuple_id.
     * @note A shared lock of the tuple should be held by txn.
     * @note The current thread should be blocked on waiting.
     *
     * @param txn the transaction requesting the lock conversion
     * @param tuple_id
     * @return true if the conversion is successful
     * @return false if:
     * 1) the transaction is not in growing phase
     * 2) the transaction does not hold a shared lock
     * 3) another transaction is trying to convert its lock
     */
    bool lock_convert(transaction::Transaction *txn, tuple_id_t tuple_id);

    /**
     * @brief Release the lock on tuple_id.
     * @note txn should actually hold the lock on tuple_id.
     *
     * @param txn the transaction
     * @param tuple_id
     * @return true if the unlock is successful
     * @return false if:
     * 1) the transaction is in growing phase
     * 2) the transaction does not hold a lock
     */
    bool unlock(transaction::Transaction *txn, tuple_id_t tuple_id);

    /**
     * @brief Build the wait-for graph according to the lock table.
     *
     * @return Graph<txn_id_t>
     */
    Graph<txn_id_t> build_graph() const;

    /**
     * @brief Check if there is a cycle in the graph.
     *
     * @param graph
     * @return txn_id_t INVALID_TXN_ID if the graph is acyclic. Otherwise, return the vertex with the largest txn id in
     * the cycles.
     */
    txn_id_t has_cycle(const Graph<txn_id_t> &graph) const;

  private:
    static constexpr size_t DEADLOCK_DETECTION_INTERVAL = 100;

    /**
     * @brief This is the background thread that detects deadlock periodically.
     *
     */
    void deadlock_detection();

    // make this protected for testing
  protected:
    std::unordered_map<tuple_id_t, LockList> lock_table_;

  private:
    std::mutex latch_;

    bool enable_deadlock_detection_;
    std::atomic<bool> stop_deadlock_detector_;
    std::thread deadlock_detector_;
};
}  // namespace naivedb::lock