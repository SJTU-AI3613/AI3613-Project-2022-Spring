#include "transaction/transaction_manager.h"

#include "buffer/buffer_manager.h"
#include "common/macros.h"
#include "lock/lock_manager.h"
#include "log/log_manager.h"
#include "log/log_record.h"
#include "storage/table/table_heap.h"
#include "storage/table/table_page.h"
#include "storage/tuple/tuple.h"
#include "transaction/transaction.h"

#include <memory>
#include <shared_mutex>
#include <unordered_map>

namespace naivedb::transaction {
static std::unordered_map<txn_id_t, std::unique_ptr<Transaction>> global_txn_map;
static std::shared_mutex global_txn_map_latch;

TransactionManager::~TransactionManager() {
    std::unique_lock latch(global_txn_map_latch);
    global_txn_map.clear();
}

Transaction *TransactionManager::begin_transaction() {
    auto txn_id = next_txn_id_.fetch_add(1);
    Transaction *txn = nullptr;
    {
        std::unique_lock latch(global_txn_map_latch);
        auto [iter, _] = global_txn_map.emplace(txn_id, std::make_unique<Transaction>(txn_id));
        txn = iter->second.get();
    }
    if (log_manager_) {
        auto lsn = log_manager_->append_record(log::LogRecord(log::LogRecordType::Begin, txn_id, txn->lsn()));
        txn->set_lsn(lsn);
    }
    return txn;
}

void TransactionManager::abort_transaction(txn_id_t txn_id) {
    auto txn = get_transaction(txn_id);
    if (!txn) {
        return;
    }
    if (log_manager_) {
        auto lsn = log_manager_->append_record(log::LogRecord(log::LogRecordType::Abort, txn_id, txn->lsn()));
        txn->set_lsn(lsn);
    }
    txn->set_state(TransactionState::Aborted);
    rollback(txn);
    release_all_locks(txn);
}

void TransactionManager::commit_transaction(txn_id_t txn_id) {
    auto txn = get_transaction(txn_id);
    if (!txn) {
        return;
    }
    if (log_manager_) {
        auto lsn = log_manager_->append_record(log::LogRecord(log::LogRecordType::Commit, txn_id, txn->lsn()));
        log_manager_->flush();
        txn->set_lsn(lsn);
    }
    txn->set_state(TransactionState::Committed);
    release_all_locks(txn);
}

Transaction *TransactionManager::get_transaction(txn_id_t txn_id) {
    std::shared_lock latch(global_txn_map_latch);
    if (auto iter = global_txn_map.find(txn_id); iter != global_txn_map.end()) {
        return iter->second.get();
    }
    return nullptr;
}

void TransactionManager::rollback(Transaction *txn) {
    if (!log_manager_ || !buffer_manager_) {
        return;
    }
    // TODO(Project-2 Part-C): Implement this method
    // iterate back from txn's lsn and use TablePage's method to undo the update
    UNIMPLEMENTED;
}

void TransactionManager::release_all_locks(Transaction *txn) {
    std::unordered_set<tuple_id_t> lock_set;
    for (auto &tuple_id : txn->exclusive_lock_set()) {
        lock_set.emplace(tuple_id);
    }
    for (auto &tuple_id : txn->shared_lock_set()) {
        lock_set.emplace(tuple_id);
    }
    for (auto &tuple_id : lock_set) {
        lock_manager_->unlock(txn, tuple_id);
    }
}
}  // namespace naivedb::transaction