#pragma once

#include "common/constants.h"
#include "common/macros.h"
#include "common/types.h"

#include <unordered_set>

namespace naivedb::transaction {
// Since we follow SS2PL, we don't need an explicit shrinking phase.
enum class TransactionState {
    Growing,
    Committed,
    Aborted,
};

/**
 * @brief The representation of a transaction.
 *
 */
class Transaction {
    DISALLOW_COPY_AND_MOVE(Transaction)
  public:
    Transaction(txn_id_t txn_id) : txn_id_(txn_id), lsn_(INVALID_LSN), state_(TransactionState::Growing) {}

    txn_id_t transaction_id() const { return txn_id_; }

    lsn_t lsn() const { return lsn_; }

    void set_lsn(lsn_t lsn) { lsn_ = lsn; }

    TransactionState state() const { return state_; }

    void set_state(TransactionState state) { state_ = state; }

    bool is_shared_locked(tuple_id_t tuple_id) const {
        return shared_lock_set_.find(tuple_id) != shared_lock_set_.end();
    }

    bool is_exclusive_locked(tuple_id_t tuple_id) const {
        return exclusive_lock_set_.find(tuple_id) != exclusive_lock_set_.end();
    }

    std::unordered_set<tuple_id_t> &shared_lock_set() { return shared_lock_set_; }

    std::unordered_set<tuple_id_t> &exclusive_lock_set() { return exclusive_lock_set_; }

  private:
    txn_id_t txn_id_;
    lsn_t lsn_;
    TransactionState state_;
    std::unordered_set<tuple_id_t> shared_lock_set_;
    std::unordered_set<tuple_id_t> exclusive_lock_set_;
};
}  // namespace naivedb::transaction