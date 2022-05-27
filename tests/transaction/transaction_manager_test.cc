#include "lock/lock_test_utils.h"
#include "naivedb_prelude.h"
#include "query/query_test_utils.h"
#include "test_utils.h"
#include "transaction/transaction_test_utils.h"

#include <cstdlib>
#include <memory>

using namespace naivedb;

// UPDATE Person SET age = 999, name = 'unknown' WHERE id < 2;
std::unique_ptr<query::PhysicalUpdate> get_update_plan1(const catalog::TableInfo &table_info) {
    auto table_schema = table_info.schema();
    auto table_id = table_info.table_id();
    auto id_col_id = table_schema->column_id("id");
    auto name_col_id = table_schema->column_id("name");
    auto age_col_id = table_schema->column_id("age");
    auto &id_col = table_schema->column(id_col_id);
    auto id = std::make_unique<query::ColumnExpr>(id_col_id, id_col.type());
    auto const_2 = std::make_unique<query::ConstExpr>(type::Value(2));
    auto id_lt_1 = std::make_unique<query::BinaryExpr>(query::BinaryOperator::Lt, std::move(id), std::move(const_2));
    std::unique_ptr<query::Expr> predicate = std::move(id_lt_1);
    auto update =
        std::make_unique<query::PhysicalUpdate>(make_vector(std::make_pair(age_col_id, type::Value(999)),
                                                            std::make_pair(name_col_id, type::Value(20, "unknown"))),
                                                table_id,
                                                std::move(predicate));
    return update;
}

// UPDATE Salary SET salary = 99999;
std::unique_ptr<query::PhysicalUpdate> get_update_plan2(const catalog::TableInfo &table_info) {
    auto table_schema = table_info.schema();
    auto table_id = table_info.table_id();
    auto salary_col_id = table_schema->column_id("salary");
    auto update = std::make_unique<query::PhysicalUpdate>(
        make_vector(std::make_pair(salary_col_id, type::Value(99999))), table_id, nullptr);
    return update;
}

// SELECT * FROM xxx;
std::unique_ptr<query::PhysicalSeqScan> get_seq_scan_plan(const catalog::TableInfo &table_info) {
    auto table_schema = table_info.schema();
    auto table_id = table_info.table_id();
    return std::make_unique<query::PhysicalSeqScan>(table_schema, table_id);
}

void check_vec(const std::vector<storage::Tuple> &a, const std::vector<std::vector<type::Value>> &b) {
    TEST_ASSERT_EQ(a.size(), b.size());
    for (size_t i = 0; i < a.size(); ++i) {
        TEST_ASSERT_EQ(a[i], b[i]);
    }
}

void test_rollback() {
    fmt::print("test rollback...\n");
    BUILD_ENV
    auto table_info = build_test_table1(&buffer_manager, &catalog);

    // BEGIN TRANSACTION
    // UPDATE Person SET age = 999, name = 'unknown' WHERE id < 2;
    // ROLLBACK
    auto t1 = txn_manager.begin_transaction();
    auto update = get_update_plan1(table_info);
    query::ExecutionEngine(&buffer_manager, &catalog, t1, &lock_manager, &log_manager).execute(update.get());
    txn_manager.abort_transaction(t1->transaction_id());

    // now the transaction should be rolled back
    // create a new transaction to check if the tuples are changed

    // BEGIN TRANSACTION
    // SELECT * FROM Person;
    // COMMIT
    auto t2 = txn_manager.begin_transaction();
    auto seq_scan = get_seq_scan_plan(table_info);
    auto result =
        query::ExecutionEngine(&buffer_manager, &catalog, t2, &lock_manager, &log_manager).execute(seq_scan.get());
    txn_manager.commit_transaction(t2->transaction_id());

    // check result
    auto expect = make_vector(make_vector(type::Value(0), type::Value(20, "Alice"), type::Value(17)),
                              make_vector(type::Value(1), type::Value(20, "Bob"), type::Value(18)),
                              make_vector(type::Value(2), type::Value(20, "Carol"), type::Value(19)),
                              make_vector(type::Value(3), type::Value(20, "Dave"), type::Value(20)));
    check_vec(result, expect);
    fmt::print("passed!\n");
}

void test_query1() {
    fmt::print("test query1...\n");
    BUILD_ENV
    auto table_info = build_test_table1(&buffer_manager, &catalog);

    // BEGIN TRANSACTION
    // SELECT * FROM Person;
    // UPDATE Person SET age = 999, name = 'unknown' WHERE id < 2;
    // SELECT * FROM Person;
    // COMMIT
    auto t1 = txn_manager.begin_transaction();
    auto e1 = query::ExecutionEngine(&buffer_manager, &catalog, t1, &lock_manager, &log_manager);
    auto seq_scan = get_seq_scan_plan(table_info);
    auto update = get_update_plan1(table_info);
    auto result1 = e1.execute(seq_scan.get());
    auto result2 = e1.execute(update.get());
    auto result3 = e1.execute(seq_scan.get());
    txn_manager.commit_transaction(t1->transaction_id());

    auto expect1 = make_vector(make_vector(type::Value(0), type::Value(20, "Alice"), type::Value(17)),
                               make_vector(type::Value(1), type::Value(20, "Bob"), type::Value(18)),
                               make_vector(type::Value(2), type::Value(20, "Carol"), type::Value(19)),
                               make_vector(type::Value(3), type::Value(20, "Dave"), type::Value(20)));
    check_vec(result1, expect1);
    TEST_ASSERT(result2.empty());
    auto expect3 = make_vector(make_vector(type::Value(0), type::Value(20, "unknown"), type::Value(999)),
                               make_vector(type::Value(1), type::Value(20, "unknown"), type::Value(999)),
                               make_vector(type::Value(2), type::Value(20, "Carol"), type::Value(19)),
                               make_vector(type::Value(3), type::Value(20, "Dave"), type::Value(20)));
    check_vec(result3, expect3);

    fmt::print("passed!\n");
}

void test_query2() {
    fmt::print("test query2...\n");
    BUILD_ENV
    TaskQueue tasks;
    auto table_info1 = build_test_table1(&buffer_manager, &catalog);
    auto table_info2 = build_test_table2(&buffer_manager, &catalog);
    std::atomic<bool> t1_done(false), t2_done(false);

    // You should handle the deadlock correctly.
    // BEGIN TRANSACTION T1                 BEGIN TRANSACTION T2
    // UPDATE Salary SET salary = 99999;    UPDATE Person SET age = 999, name = 'unknown' WHERE id < 2;
    // SELECT * FROM Person;                SELECT * FROM Salary;
    // COMMIT                               COMMIT

    auto t1 = txn_manager.begin_transaction();
    auto t2 = txn_manager.begin_transaction();
    tasks
        .push([&]() {
            try {
                auto update = get_update_plan2(table_info2);
                auto select = get_seq_scan_plan(table_info1);
                auto e1 = query::ExecutionEngine(&buffer_manager, &catalog, t1, &lock_manager, &log_manager);
                e1.execute(update.get());
                sleep_for(200);
                e1.execute(select.get());
                txn_manager.commit_transaction(t1->transaction_id());
            } catch (TransactionAbortException &e) {
                txn_manager.abort_transaction(e.transaction_id());
            }
            t1_done.store(true);
        })
        .push([&]() {
            try {
                auto update = get_update_plan2(table_info1);
                auto select = get_seq_scan_plan(table_info2);
                auto e2 = query::ExecutionEngine(&buffer_manager, &catalog, t2, &lock_manager, &log_manager);
                e2.execute(update.get());
                sleep_for(200);
                e2.execute(select.get());
                txn_manager.commit_transaction(t2->transaction_id());
            } catch (TransactionAbortException &e) {
                txn_manager.abort_transaction(e.transaction_id());
            }
            t2_done.store(true);
        })
        // if any of t1, t2 does not commit or abort after 1.0s, the test fails.
        .push([&]() {
            sleep_for(1000);
            if (!t1_done.load() || !t2_done.load()) {
                TEST_ASSERT(false);
            }
        });
    tasks.wait();

    // BEGIN TRANSACTION T3
    // SELECT * FROM Person;
    // SELECT * FROM Salary;
    // COMMIT
    auto t3 = txn_manager.begin_transaction();
    auto select_person = get_seq_scan_plan(table_info1);
    auto select_salary = get_seq_scan_plan(table_info2);
    auto e3 = query::ExecutionEngine(&buffer_manager, &catalog, t3, &lock_manager, &log_manager);
    auto result1 = e3.execute(select_person.get());
    auto result2 = e3.execute(select_salary.get());
    txn_manager.commit_transaction(t3->transaction_id());
    auto expect1 = make_vector(make_vector(type::Value(0), type::Value(20, "Alice"), type::Value(17)),
                               make_vector(type::Value(1), type::Value(20, "Bob"), type::Value(18)),
                               make_vector(type::Value(2), type::Value(20, "Carol"), type::Value(19)),
                               make_vector(type::Value(3), type::Value(20, "Dave"), type::Value(20)));
    auto expect2 =
        make_vector(make_vector(type::Value(2), type::Value(99999)), make_vector(type::Value(3), type::Value(99999)));

    // T2 should be aborted
    check_vec(result1, expect1);
    check_vec(result2, expect2);

    fmt::print("passed!\n");
}

int main(int argc, char *argv[]) {
    std::vector<std::pair<std::string_view, std::function<void()>>> test_f{
        {"rollback", test_rollback},
        {"query1", test_query1},
        {"query2", test_query2},
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