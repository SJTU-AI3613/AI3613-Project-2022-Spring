#pragma once

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "catalog/table_info.h"
#include "common/utils.h"
#include "storage/table/table_heap.h"
#include "type/value.h"

namespace naivedb {
inline catalog::TableInfo build_test_table1(buffer::BufferManager *buffer_manager, catalog::Catalog *catalog) {
    auto table_id = catalog->create_table("Person",
                                          catalog::Schema({
                                              {"id", type::Type(type::Int())},
                                              {"name", type::Type(type::Char(20))},
                                              {"age", type::Type(type::Int())},
                                          }));
    auto table_info = catalog->get_table_info(table_id);

    storage::TableHeap table_heap(buffer_manager, table_info.root_page_id());
    auto values = make_vector(make_vector(type::Value(0), type::Value(20, "Alice"), type::Value(17)),
                              make_vector(type::Value(1), type::Value(20, "Bob"), type::Value(18)),
                              make_vector(type::Value(2), type::Value(20, "Carol"), type::Value(19)),
                              make_vector(type::Value(3), type::Value(20, "Dave"), type::Value(20)));

    std::vector<storage::Tuple> tuples;
    for (auto &row_values : values) {
        tuples.emplace_back(row_values);
    }
    for (auto &tuple : tuples) {
        table_heap.insert_tuple(tuple);
    }

    return table_info;
}

inline catalog::TableInfo build_test_table2(buffer::BufferManager *buffer_manager, catalog::Catalog *catalog) {
    auto table_id = catalog->create_table("Salary",
                                          catalog::Schema({
                                              {"id", type::Type(type::Int())},
                                              {"salary", type::Type(type::Int())},
                                          }));
    auto table_info = catalog->get_table_info(table_id);

    storage::TableHeap table_heap(buffer_manager, table_info.root_page_id());
    auto values =
        make_vector(make_vector(type::Value(2), type::Value(100)), make_vector(type::Value(3), type::Value(200)));

    std::vector<storage::Tuple> tuples;
    for (auto &row_values : values) {
        tuples.emplace_back(row_values);
    }
    for (auto &tuple : tuples) {
        table_heap.insert_tuple(tuple);
    }

    return table_info;
}

inline catalog::TableInfo build_test_table3(buffer::BufferManager *buffer_manager, catalog::Catalog *catalog) {
    auto table_id = catalog->create_table("Grade",
                                          catalog::Schema({
                                              {"student_id", type::Type(type::Int())},
                                              {"course", type::Type(type::Char(10))},
                                              {"grade", type::Type(type::Int())},
                                          }));
    auto table_info = catalog->get_table_info(table_id);

    storage::TableHeap table_heap(buffer_manager, table_info.root_page_id());
    auto values = make_vector(make_vector(type::Value(0), type::Value(10, "Math"), type::Value(80)),
                              make_vector(type::Value(0), type::Value(10, "Physics"), type::Value(85)),
                              make_vector(type::Value(0), type::Value(10, "Chemistry"), type::Value(90)),
                              make_vector(type::Value(1), type::Value(10, "Math"), type::Value(60)),
                              make_vector(type::Value(1), type::Value(10, "Physics"), type::Value(65)),
                              make_vector(type::Value(1), type::Value(10, "Chemistry"), type::Value(70)),
                              make_vector(type::Value(2), type::Value(10, "Math"), type::Value(90)),
                              make_vector(type::Value(2), type::Value(10, "Physics"), type::Value(95)),
                              make_vector(type::Value(2), type::Value(10, "Chemistry"), type::Value(100)));

    std::vector<storage::Tuple> tuples;
    for (auto &row_values : values) {
        tuples.emplace_back(row_values);
    }
    for (auto &tuple : tuples) {
        table_heap.insert_tuple(tuple);
    }

    return table_info;
}
}  // namespace naivedb