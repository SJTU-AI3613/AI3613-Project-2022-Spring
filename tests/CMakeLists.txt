macro(add_test_exec exec_name)
    add_executable(${exec_name} ${exec_name}.cc)
    target_link_libraries(${exec_name} naivedb)
endmacro(add_test_exec)

include_directories(${PROJECT_SOURCE_DIR}/tests)

add_subdirectory(io)
add_subdirectory(buffer)
add_subdirectory(storage)
add_subdirectory(query)
add_subdirectory(catalog)
add_subdirectory(common)