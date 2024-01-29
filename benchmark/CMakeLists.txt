cmake_minimum_required(VERSION 3.2)

project(TANTIVY_SEARCH_BENCHMARK)

file(GLOB BENCHMARK_ENTRY ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)
file(GLOB BENCHMARKS ${CMAKE_CURRENT_SOURCE_DIR}/*)

message(STATUS "Config Google Benchmark")

add_executable(tantivy_search_benchmark ${BENCHMARK_ENTRY} ${BENCHMARKS})
target_include_directories(tantivy_search_benchmark PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(tantivy_search_benchmark PUBLIC tantivy_search_contrib_lib rust::tantivy_search)
