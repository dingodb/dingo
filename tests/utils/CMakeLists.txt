cmake_minimum_required(VERSION 3.2)

project(TANTIVY_SEARCH_TESTS_UTILS)

file(GLOB SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/*)

message(STATUS "Config tantivy_search / tests / utils.")

add_library(tests_utils ${SOURCES})
target_include_directories(tests_utils PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(tests_utils PUBLIC tantivy_search_contrib_lib)
