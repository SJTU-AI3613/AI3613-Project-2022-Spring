file(GLOB NAIVEDB_SOURCES
    "io/*.cc"
    "buffer/*.cc"
    "type/*.cc"
    "catalog/*.cc"
    "query/expr/*.cc"
    "query/execution/*.cc"
    "query/execution/executor/*.cc"
    "storage/table/*.cc"
    "storage/tuple/*.cc")
add_library(naivedb STATIC ${NAIVEDB_SOURCES})

# link fmt
target_link_libraries(naivedb fmt::fmt)

set_target_properties(naivedb PROPERTIES ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})