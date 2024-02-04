cmake_minimum_required(VERSION 3.2)

project(TANTIVY_SEARCH_UNIT_TESTS)

file(GLOB TESTS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

message(STATUS "Config unit tests.")

add_executable(
    unit_tests
    ${TESTS}
)

target_link_libraries(unit_tests
  PRIVATE
  rust::tantivy_search
  tests_utils
  gtest_main
)