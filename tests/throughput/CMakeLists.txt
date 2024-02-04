cmake_minimum_required(VERSION 3.2)

project(TANTIVY_SEARCH_DEMO_UTILS)

file(GLOB SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/*)

message(STATUS "Config throughput test.")

add_executable(throughput_test ${SOURCES})
target_include_directories(throughput_test PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(throughput_test PRIVATE rust::tantivy_search tests_utils)
