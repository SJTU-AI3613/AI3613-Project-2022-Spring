#include "lock/lock_test_utils.h"
#include "naivedb_prelude.h"
#include "test_utils.h"

#include <chrono>
#include <cstdlib>
#include <functional>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <vector>

using namespace naivedb;

void test_exclusive() {
    fmt::print("test exclusive...\n");
    lock::LockManager lm(false);
    transaction::TransactionManager tm(&lm);
    auto txn = tm.begin_transaction();
    txn->set_state(transaction::TransactionState::Committed);

    // SS2PL violation check
    TEST_ASSERT(!lm.lock_exclusive(txn, 123));
    TEST_ASSERT_EQ(txn->exclusive_lock_set(), std::unordered_set<tuple_id_t>{});

    // basic locking check
    txn->set_state(transaction::TransactionState::Growing);
    TEST_ASSERT(lm.lock_exclusive(txn, 123));
    TEST_ASSERT_EQ(txn->exclusive_lock_set(), std::unordered_set<tuple_id_t>{123});
    TEST_ASSERT(lm.lock_exclusive(txn, 456));
    TEST_ASSERT_EQ(txn->exclusive_lock_set(), (std::unordered_set<tuple_id_t>{123, 456}));

    // double lock check
    TEST_ASSERT(!lm.lock_exclusive(txn, 123));
    TEST_ASSERT_EQ(txn->exclusive_lock_set(), (std::unordered_set<tuple_id_t>{123, 456}));

    fmt::print("passed!\n");
}

void test_convert() {
    fmt::print("test convert...\n");
    lock::LockManager lm(false);
    transaction::TransactionManager tm(&lm);
    auto txn = tm.begin_transaction();
    txn->set_state(transaction::TransactionState::Committed);

    // SS2PL violation check
    TEST_ASSERT(!lm.lock_convert(txn, 123));
    TEST_ASSERT_EQ(txn->exclusive_lock_set(), std::unordered_set<tuple_id_t>{});

    // no shared lock to convert
    txn->set_state(transaction::TransactionState::Growing);
    TEST_ASSERT(!lm.lock_convert(txn, 123));
    TEST_ASSERT_EQ(txn->exclusive_lock_set(), std::unordered_set<tuple_id_t>{});

    // basic conversion check
    TEST_ASSERT(lm.lock_shared(txn, 123));
    TEST_ASSERT(lm.lock_convert(txn, 123));
    TEST_ASSERT_EQ(txn->shared_lock_set(), (std::unordered_set<tuple_id_t>{}));
    TEST_ASSERT_EQ(txn->exclusive_lock_set(), (std::unordered_set<tuple_id_t>{123}));

    // no shared lock to convert
    TEST_ASSERT(!lm.lock_convert(txn, 123));
    TEST_ASSERT_EQ(txn->shared_lock_set(), (std::unordered_set<tuple_id_t>{}));
    TEST_ASSERT_EQ(txn->exclusive_lock_set(), (std::unordered_set<tuple_id_t>{123}));

    fmt::print("passed!\n");
}

void test_unlock() {
    fmt::print("test unlock...\n");
    lock::LockManager lm(false);
    transaction::TransactionManager tm(&lm);
    auto txn = tm.begin_transaction();

    // SS2PL violation check
    TEST_ASSERT(!lm.unlock(txn, 123));
    TEST_ASSERT_EQ(txn->shared_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(txn->exclusive_lock_set(), std::unordered_set<tuple_id_t>{});

    // no lock to unlock
    txn->set_state(transaction::TransactionState::Committed);
    TEST_ASSERT(!lm.unlock(txn, 123));
    TEST_ASSERT_EQ(txn->shared_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(txn->exclusive_lock_set(), std::unordered_set<tuple_id_t>{});

    // basic unlock check
    txn->set_state(transaction::TransactionState::Growing);
    TEST_ASSERT(lm.lock_shared(txn, 123));
    TEST_ASSERT(lm.lock_exclusive(txn, 456));
    txn->set_state(transaction::TransactionState::Committed);
    TEST_ASSERT(lm.unlock(txn, 123));
    TEST_ASSERT_EQ(txn->shared_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(txn->exclusive_lock_set(), std::unordered_set<tuple_id_t>{456});
    TEST_ASSERT(lm.unlock(txn, 456));
    TEST_ASSERT_EQ(txn->shared_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(txn->exclusive_lock_set(), std::unordered_set<tuple_id_t>{});

    // no lock to unlock
    TEST_ASSERT(!lm.unlock(txn, 123));
    TEST_ASSERT(!lm.unlock(txn, 456));

    fmt::print("passed!\n");
}

void test_convert_conflict() {
    fmt::print("test convert_conflict...\n");
    lock::LockManager lm(false);
    transaction::TransactionManager tm(&lm);
    auto t1 = tm.begin_transaction();
    auto t2 = tm.begin_transaction();
    auto t3 = tm.begin_transaction();
    TaskQueue tasks;
    std::atomic<bool> t1_done(false), t2_done(false);

    // This schedule should not cause a deadlock
    //    T1                  T2                  T3
    //                                         LOCK-S(0)
    // LOCK-S(0)
    //                     LOCK-S(0)
    // LOCK-CONVERT(0)
    //                     LOCK-CONVERT(0)     COMMIT
    //                                         UNLOCK(0)
    tasks
        .push([&]() {
            sleep_for(200);
            TEST_ASSERT(lm.lock_shared(t1, 0));
            sleep_for(100);
            auto txn_id = t1->transaction_id();
            try {
                ABORT_IF(!lm.lock_convert(t1, 0));
            } catch (const TransactionAbortException &e) {
                tm.abort_transaction(e.transaction_id());
            }
            t1_done.store(true);
        })
        .push([&]() {
            sleep_for(200);
            TEST_ASSERT(lm.lock_shared(t2, 0));
            auto txn_id = t2->transaction_id();
            try {
                ABORT_IF(!lm.lock_convert(t2, 0));
            } catch (const TransactionAbortException &e) {
                tm.abort_transaction(e.transaction_id());
            }
            t2_done.store(true);
        })
        .push([&]() {
            TEST_ASSERT(lm.lock_shared(t3, 0));
            sleep_for(500);
            t3->set_state(transaction::TransactionState::Committed);
            TEST_ASSERT(lm.unlock(t3, 0));
        })
        // if any of t1 and t2 does not return after 1.0s, the test fails.
        .push([&]() {
            sleep_for(1000);
            if (!t1_done.load() || !t2_done.load()) {
                TEST_ASSERT(false);
            }
        });

    tasks.wait();

    fmt::print("passed!\n");
}

void test_concurrent1() {
    fmt::print("test concurrent1...\n");
    lock::LockManager lm(false);
    transaction::TransactionManager tm(&lm);
    auto t1 = tm.begin_transaction();
    auto t2 = tm.begin_transaction();
    std::vector<int> v{100, 200};
    TaskQueue tasks;

    // T1:
    // LOCK-S(v[0])
    // x <- v[0] - 50
    // LOCK-CONVERT(v[0])
    // v[0] <- x
    // LOCK-S(v[1])
    // y <- v[1] + 50
    // LOCK-CONVERT(v[1])
    // v[1] <- y
    // COMMIT
    // UNLOCK(v[0])
    // UNLOCK(v[1])
    tasks.push([&]() {
        TEST_ASSERT(lm.lock_shared(t1, 0));
        int x = v[0] - 50;
        TEST_ASSERT(lm.lock_convert(t1, 0));
        v[0] = x;
        TEST_ASSERT(lm.lock_shared(t1, 1));
        int y = v[1] + 50;
        TEST_ASSERT(lm.lock_convert(t1, 1));
        v[1] = y;
        t1->set_state(transaction::TransactionState::Committed);
        TEST_ASSERT(lm.unlock(t1, 0));
        TEST_ASSERT(lm.unlock(t1, 1));
    });

    // T2:
    // LOCK-S(v[0])
    // x <- v[0]
    // LOCK-S(v[1])
    // y <- v[1]
    // result <- x + y
    // COMMIT
    // UNLOCK(v[0])
    // UNLOCK(v[1])
    int result;
    tasks.push([&]() {
        TEST_ASSERT(lm.lock_shared(t2, 0));
        int x = v[0];
        TEST_ASSERT(lm.lock_shared(t2, 1));
        int y = v[1];
        result = x + y;
        t2->set_state(transaction::TransactionState::Committed);
        TEST_ASSERT(lm.unlock(t2, 0));
        TEST_ASSERT(lm.unlock(t2, 1));
    });

    tasks.wait();
    TEST_ASSERT_EQ(t1->exclusive_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(t1->shared_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(t2->exclusive_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(t2->shared_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(result, 300);

    fmt::print("passed!\n");
}

void test_concurrent2() {
    fmt::print("test concurrent2...\n");
    lock::LockManager lm(false);
    transaction::TransactionManager tm(&lm);
    auto t1 = tm.begin_transaction();
    auto t2 = tm.begin_transaction();
    auto t3 = tm.begin_transaction();
    std::vector<int> v{10, 0, 0};
    TaskQueue tasks;

    // T1:
    // LOCK-X(v[0])
    // v[0] <- v[0] - 3
    // LOCK-X(v[1])
    // v[1] <- v[1] + 3
    // COMMIT
    // UNLOCK(v[1])
    // UNLOCK(v[0])
    tasks.push([&]() {
        TEST_ASSERT(lm.lock_exclusive(t1, 0));
        v[0] -= 3;
        TEST_ASSERT(lm.lock_exclusive(t1, 1));
        v[1] += 3;
        t1->set_state(transaction::TransactionState::Committed);
        TEST_ASSERT(lm.unlock(t1, 1));
        TEST_ASSERT(lm.unlock(t1, 0));
    });

    // T2:
    // LOCK-X(v[0])
    // v[0] <- v[0] - 1
    // LOCK-X(v[2])
    // v[2] <- v[2] + 1
    // COMMIT
    // UNLOCK(v[2])
    // UNLOCK(v[0])
    tasks.push([&]() {
        TEST_ASSERT(lm.lock_exclusive(t2, 0));
        v[0] -= 1;
        TEST_ASSERT(lm.lock_exclusive(t2, 2));
        v[1] += 1;
        t2->set_state(transaction::TransactionState::Committed);
        TEST_ASSERT(lm.unlock(t2, 2));
        TEST_ASSERT(lm.unlock(t2, 0));
    });

    // T3:
    // LOCK-S(v[0])
    // LOCK-S(v[1])
    // LOCK-S(v[2])
    // result <- v[0] + v[1] + v[2]
    // COMMIT
    // UNLOCK(v[2])
    // UNLOCK(v[1])
    // UNLOCK(v[0])
    int result;
    tasks.push([&]() {
        TEST_ASSERT(lm.lock_shared(t3, 0));
        TEST_ASSERT(lm.lock_shared(t3, 1));
        TEST_ASSERT(lm.lock_shared(t3, 2));
        result = v[0] + v[1] + v[2];
        t3->set_state(transaction::TransactionState::Committed);
        TEST_ASSERT(lm.unlock(t3, 2));
        TEST_ASSERT(lm.unlock(t3, 1));
        TEST_ASSERT(lm.unlock(t3, 0));
    });

    tasks.wait();
    TEST_ASSERT_EQ(t1->exclusive_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(t1->shared_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(t2->exclusive_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(t2->shared_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(t3->exclusive_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(t3->shared_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(result, 10);

    fmt::print("passed!\n");
}

void test_concurrent3() {
    fmt::print("test concurrent3...\n");
    lock::LockManager lm(false);
    transaction::TransactionManager tm(&lm);
    auto t1 = tm.begin_transaction();
    auto t2 = tm.begin_transaction();
    auto t3 = tm.begin_transaction();
    std::vector<int> v{3, 4};
    TaskQueue tasks;

    // T1:
    // LOCK-S(v[0])
    // x <- v[0]
    // LOCK-CONVERT(v[0])
    // v[0] <- v[0] + 5
    // LOCK-X(v[1])
    // v[1] <- x
    // COMMIT
    // UNLOCK(v[1])
    // UNLOCK(v[0])
    tasks.push([&]() {
        TEST_ASSERT(lm.lock_shared(t1, 0));
        int x = v[0];
        TEST_ASSERT(lm.lock_convert(t1, 0));
        v[0] += 5;
        TEST_ASSERT(lm.lock_exclusive(t1, 1));
        v[1] = x;
        t1->set_state(transaction::TransactionState::Committed);
        TEST_ASSERT(lm.unlock(t1, 1));
        TEST_ASSERT(lm.unlock(t1, 0));
    });

    // T2:
    // LOCK-X(v[0])
    // v[0] <- 10
    // COMMIT
    // UNLOCK(v[0])
    tasks.push([&]() {
        TEST_ASSERT(lm.lock_exclusive(t2, 0));
        v[0] = 10;
        t2->set_state(transaction::TransactionState::Committed);
        TEST_ASSERT(lm.unlock(t2, 0));
    });

    // T3:
    // LOCK-S(v[0])
    // LOCK-S(v[1])
    // result <- v[0] + v[1]
    // COMMIT
    // UNLOCK(v[1])
    // UNLOCK(v[0])
    int result;
    tasks.push([&]() {
        TEST_ASSERT(lm.lock_shared(t3, 0));
        TEST_ASSERT(lm.lock_shared(t3, 1));
        result = v[0] + v[1];
        t3->set_state(transaction::TransactionState::Committed);
        TEST_ASSERT(lm.unlock(t3, 1));
        TEST_ASSERT(lm.unlock(t3, 0));
    });

    tasks.wait();
    TEST_ASSERT_EQ(t1->exclusive_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(t1->shared_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(t2->exclusive_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(t2->shared_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(t3->exclusive_lock_set(), std::unordered_set<tuple_id_t>{});
    TEST_ASSERT_EQ(t3->shared_lock_set(), std::unordered_set<tuple_id_t>{});
    // serial schedule, result
    // (T1, T2, T3), 13
    // (T1, T3, T2), 11
    // (T2, T1, T3), 25
    // (T2, T3, T1), 14
    // (T3, *, *), 7
    std::unordered_set<int> result_set{13, 11, 25, 14, 7};
    TEST_ASSERT_NE(result_set.find(result), result_set.end());

    fmt::print("passed!\n");
}

void test_graph() {
    fmt::print("test graph...\n");
    TestLockManager lm;
    auto graph = lm.add_exclusive_lock(0, 1)
                     .add_exclusive_lock(1, 1)
                     .add_shared_lock(2, 1)
                     .add_shared_lock(2, 2)
                     .add_shared_lock(2, 3)
                     .add_shared_lock(3, 1)
                     .add_wait(1, 2, TestLockManager::LockMode::Shared)
                     .add_wait(2, 1, TestLockManager::LockMode::Exclusive)
                     .add_wait(2, 4, TestLockManager::LockMode::Exclusive)
                     .build_graph();

    TEST_ASSERT_EQ(graph.vertices().size(), 4);
    TEST_ASSERT_EQ(graph.outgoing_neighbors(1), (std::unordered_set<txn_id_t>{2, 3}));
    TEST_ASSERT_EQ(graph.outgoing_neighbors(2), (std::unordered_set<txn_id_t>{1}));
    TEST_ASSERT_EQ(graph.outgoing_neighbors(3), (std::unordered_set<txn_id_t>{}));
    TEST_ASSERT_EQ(graph.outgoing_neighbors(4), (std::unordered_set<txn_id_t>{1, 2, 3}));

    fmt::print("passed!\n");
}

void test_cycle1() {
    fmt::print("test cycle1...\n");
    lock::LockManager lm(false);
    Graph<txn_id_t> graph;

    graph.add_vertex(1)
        .add_vertex(2)
        .add_vertex(3)
        .add_vertex(4)
        .add_vertex(5)
        .add_vertex(6)
        .add_edge(1, 2)
        .add_edge(3, 2)
        .add_edge(3, 6)
        .add_edge(4, 3)
        .add_edge(4, 5)
        .add_edge(4, 6)
        .add_edge(5, 1)
        .add_edge(6, 1)
        .add_edge(6, 5);

    TEST_ASSERT_EQ(lm.has_cycle(graph), INVALID_TXN_ID);

    fmt::print("passed!\n");
}

void test_cycle2() {
    fmt::print("test cycle2...\n");
    lock::LockManager lm(false);
    Graph<txn_id_t> graph;

    graph.add_vertex(1)
        .add_vertex(2)
        .add_vertex(3)
        .add_vertex(4)
        .add_vertex(5)
        .add_vertex(6)
        .add_vertex(7)
        .add_edge(1, 3)
        .add_edge(1, 4)
        .add_edge(2, 1)
        .add_edge(2, 3)
        .add_edge(2, 4)
        .add_edge(3, 5)
        .add_edge(4, 6)
        .add_edge(5, 4)
        .add_edge(6, 3)
        .add_edge(7, 6);

    TEST_ASSERT_EQ(lm.has_cycle(graph), 6);

    fmt::print("passed!\n");
}

void test_deadlock1() {
    fmt::print("test deadlock1...\n");
    lock::LockManager lm(true);
    transaction::TransactionManager tm(&lm);
    auto t1 = tm.begin_transaction();
    auto t2 = tm.begin_transaction();
    TaskQueue tasks;
    std::atomic<bool> t1_done(false), t2_done(false);

    //    T1                  T2
    // LOCK-S(0)
    //                     LOCK-S(1)
    //                     LOCK-X(0)
    // LOCK-X(1)
    // COMMIT              COMMIT
    // UNLOCK(0)           UNLOCK(0)
    // UNLOCK(1)           UNLOCK(1)
    tasks
        .push([&]() {
            auto txn_id = t1->transaction_id();
            TEST_ASSERT(lm.lock_shared(t1, 0));
            sleep_for(200);
            try {
                ABORT_IF(!lm.lock_exclusive(t1, 1));
                t1->set_state(transaction::TransactionState::Committed);
                lm.unlock(t1, 0);
                lm.unlock(t1, 1);
            } catch (const TransactionAbortException &e) {
                tm.abort_transaction(e.transaction_id());
            }
            t1_done.store(true);
        })
        .push([&]() {
            sleep_for(100);
            auto txn_id = t2->transaction_id();
            try {
                ABORT_IF(!lm.lock_shared(t2, 1));
                ABORT_IF(!lm.lock_exclusive(t2, 0));
                t2->set_state(transaction::TransactionState::Committed);
                lm.unlock(t2, 0);
                lm.unlock(t2, 1);
            } catch (const TransactionAbortException &e) {
                tm.abort_transaction(e.transaction_id());
            }
            t2_done.store(true);
        })
        // if any of t1 and t2 does not commit or abort after 1.0s, the test fails.
        .push([&]() {
            sleep_for(1000);
            if (!t1_done.load() || !t2_done.load()) {
                TEST_ASSERT(false);
            }
        });
    tasks.wait();

    fmt::print("passed!\n");
}

void test_deadlock2() {
    fmt::print("test deadlock2...\n");
    lock::LockManager lm(true);
    transaction::TransactionManager tm(&lm);
    auto t1 = tm.begin_transaction();
    auto t2 = tm.begin_transaction();
    auto t3 = tm.begin_transaction();
    TaskQueue tasks;
    std::atomic<bool> t1_done(false), t2_done(false), t3_done(false);

    //    T1                  T2                  T3
    // LOCK-S(0)
    //                     LOCK-X(1)
    //                                         LOCK-S(2)
    // LOCK-S(1)
    //                     LOCK-X(2)
    //                                         LOCK-X(0)
    // COMMIT              COMMIT              COMMIT
    // UNLOCK(0)           UNLOCK(1)           UNLOCK(2)
    // UNLOCK(1)           UNLOCK(2)           UNLOCK(0)
    tasks
        .push([&]() {
            auto txn_id = t1->transaction_id();
            TEST_ASSERT(lm.lock_shared(t1, 0));
            sleep_for(400);
            try {
                ABORT_IF(!lm.lock_shared(t1, 1));
                t1->set_state(transaction::TransactionState::Committed);
                lm.unlock(t1, 0);
                lm.unlock(t1, 1);
            } catch (const TransactionAbortException &e) {
                tm.abort_transaction(e.transaction_id());
            }
            t1_done.store(true);
        })
        .push([&]() {
            auto txn_id = t2->transaction_id();
            sleep_for(100);
            TEST_ASSERT(lm.lock_exclusive(t2, 1));
            sleep_for(400);
            try {
                ABORT_IF(!lm.lock_exclusive(t2, 2));
                t2->set_state(transaction::TransactionState::Committed);
                lm.unlock(t2, 1);
                lm.unlock(t2, 2);
            } catch (const TransactionAbortException &e) {
                tm.abort_transaction(e.transaction_id());
            }
            t2_done.store(true);
        })
        .push([&]() {
            auto txn_id = t3->transaction_id();
            sleep_for(200);
            TEST_ASSERT(lm.lock_shared(t3, 2));
            sleep_for(400);
            try {
                ABORT_IF(!lm.lock_exclusive(t3, 0));
                t3->set_state(transaction::TransactionState::Committed);
                lm.unlock(t1, 2);
                lm.unlock(t1, 0);
            } catch (const TransactionAbortException &e) {
                tm.abort_transaction(e.transaction_id());
            }
            t3_done.store(true);
        })
        // if any of t1, t2 and t3 does not commit or abort after 1.0s, the test fails.
        .push([&]() {
            sleep_for(1000);
            if (!t1_done.load() || !t2_done.load() || !t3_done.load()) {
                TEST_ASSERT(false);
            }
        });
    tasks.wait();

    fmt::print("passed!\n");
}

int main(int argc, char *argv[]) {
    std::vector<std::pair<std::string_view, std::function<void()>>> test_f{
        {"exclusive", test_exclusive},
        {"convert", test_convert},
        {"unlock", test_unlock},
        {"convert_conflict", test_convert_conflict},
        {"concurrent1", test_concurrent1},
        {"concurrent2", test_concurrent2},
        {"concurrent3", test_concurrent3},
        {"graph", test_graph},
        {"cycle1", test_cycle1},
        {"cycle2", test_cycle2},
        {"deadlock1", test_deadlock1},
        {"deadlock2", test_deadlock2},
    };
    if (argc != 2) {
        fmt::print("usage: {} <testcase>\n<testcase> can be:\n", argv[0]);
        for (auto &[name, _] : test_f) {
            fmt::print("{}\n", name);
        }
        return EXIT_FAILURE;
    }
    auto testcase = std::string_view(argv[1]);
    if (testcase == "all") {
        for (auto &[_, f] : test_f) {
            f();
        }
    } else if (auto iter = std::find_if(test_f.begin(), test_f.end(), [&](auto v) { return v.first == testcase; });
               iter != test_f.end()) {
        iter->second();
    } else {
        fmt::print("error: invalid testcase!\n");
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}