project(tantivy-search-contrib)

add_subdirectory(json)
# add_subdirectory(thread_pool)
# add_subdirectory(dependency2)

set(CONTRIB_INCLUDE_DIRS 
    ${CMAKE_CURRENT_SOURCE_DIR}/json/single_include
    ${CMAKE_CURRENT_SOURCE_DIR}/thread_pool
    ${CMAKE_CURRENT_SOURCE_DIR}/easylogging/src
    # ${CMAKE_CURRENT_SOURCE_DIR}/dependency2/include
    PARENT_SCOPE
)