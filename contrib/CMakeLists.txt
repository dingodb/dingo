cmake_minimum_required(VERSION 3.2)

project(TANTIVY_SEARCH_CONTRIB)

add_subdirectory(boost-cmake)
add_subdirectory(google_test)
add_subdirectory(benchmark)

# gather third-party include directories
set(CONTRIB_INCLUDE_DIRS 
    ${CMAKE_CURRENT_SOURCE_DIR}/json/single_include
    ${CMAKE_CURRENT_SOURCE_DIR}/thread_pool
    ${CMAKE_CURRENT_SOURCE_DIR}/easylogging/src
    ${CMAKE_CURRENT_SOURCE_DIR}/google_test/googletest/include
    ${CMAKE_CURRENT_SOURCE_DIR}/google_test/googletest
    ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/include
)

# gather third-party sources
file(GLOB_RECURSE CONTRIB_SOURCES 
    ${CMAKE_CURRENT_SOURCE_DIR}/json/single_include/*
    ${CMAKE_CURRENT_SOURCE_DIR}/thread_pool/ThreadPool.h
    ${CMAKE_CURRENT_SOURCE_DIR}/easylogging/src/*
    )

# create a library
message(STATUS "Create tantivy_search contrib library")
add_library(tantivy_search_contrib_lib ${CONTRIB_SOURCES})
target_link_libraries(tantivy_search_contrib_lib PUBLIC boost::program_options benchmark::benchmark)
target_include_directories(tantivy_search_contrib_lib PUBLIC ${CONTRIB_INCLUDE_DIRS})