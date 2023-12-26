cmake_minimum_required(VERSION 3.2)

project(TANTIVY_SEARCH_BENCHMARK)

file(GLOB BENCHMARK_ENTRY ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)
file(GLOB BENCHMARKS ${CMAKE_CURRENT_SOURCE_DIR}/*.h)

message(STATUS "Config benchmarks.")

add_executable(tantivy_search_benchmark ${BENCHMARK_ENTRY} ${BENCHMARKS})
target_link_libraries(tantivy_search_benchmark PUBLIC tantivy_search tantivy_search_contrib_lib)
