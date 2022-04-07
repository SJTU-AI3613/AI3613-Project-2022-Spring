cmake_minimum_required(VERSION 3.18)
project(NaiveDB)
enable_testing()

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING
        "Default build type: Debug" FORCE)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Use IWYU
# set(CMAKE_CXX_INCLUDE_WHAT_YOU_USE include-what-you-use)
add_compile_options(-pedantic -Wall -Werror)

# Add fmt
find_package(fmt REQUIRED)

include_directories(${PROJECT_SOURCE_DIR}/src)

add_subdirectory(${PROJECT_SOURCE_DIR}/src)
add_subdirectory(${PROJECT_SOURCE_DIR}/tests)