cmake_minimum_required(VERSION 3.2)

project(TANTIVY_SEARCH_DEMO_UTILS)

# gather source and header
file(GLOB UTILS_SOURCES *.cpp)

# create a library
message(STATUS "Create tantivy_search utils library")
add_library(tantivy_search_utils_lib ${UTILS_SOURCES})
target_include_directories(tantivy_search_utils_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
# link contrib to utils
target_link_libraries(tantivy_search_utils_lib PUBLIC tantivy_search_contrib_lib)