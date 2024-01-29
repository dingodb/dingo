cmake_minimum_required(VERSION 3.2)

project(TANTIVY_SEARCH_EXAMPLE)

add_subdirectory(utils ${CMAKE_CURRENT_BINARY_DIR}/utils)
file(GLOB DEMOS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

message(STATUS "Config examples.")

# use jemalloc
# find_library(JEMALLOC_LIB jemalloc)
# find_package(PkgConfig REQUIRED)
# pkg_check_modules (JEMALLOC jemalloc)
# pkg_search_module(JEMALLOC REQUIRED jemalloc)
# include_directories(${JEMALLOC_INCLUDE_DIRS})
# message(STATUS "find jemalloc local: ${JEMALLOC_LIBRARIES}")

# Each example will create a target
foreach(DEMO ${DEMOS})
    # get example target_name
    get_filename_component(FILE_NAME ${DEMO} NAME_WE)
    # create example target
    add_executable(${FILE_NAME} ${DEMO})
    target_link_libraries(${FILE_NAME} PUBLIC tantivy_search_utils_lib rust::tantivy_search)
endforeach()
