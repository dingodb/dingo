cmake_minimum_required(VERSION 3.2)

project(TANTIVY_LIBRARY_DEMO)

include_directories(${CONTRIB_INCLUDE_DIRS})

# 获取当前 example 目录下所有的 .cpp 文件
file(GLOB DEMOS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)


# 为每个 example 创建一个 target
foreach(DEMO ${DEMOS})
    # 获取文件名作为 example target 名称
    get_filename_component(FILE_NAME ${DEMO} NAME_WE)
    # 创建 example target
    add_executable(${FILE_NAME} ${DEMO})
    # 添加依赖，确保在构建 example target 之前已经构建了 Rust 静态库
    add_dependencies(${FILE_NAME}  tantivy_library_build_target)
    # 链接 Rust 构建出的静态库到 example target
    target_link_directories(${FILE_NAME} PRIVATE ${LIB_SEARCH_PATH})
    target_link_libraries(${FILE_NAME} PRIVATE tantivy_search)
    # 为 example target 指定 include 目录路径
    target_include_directories(${FILE_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)

endforeach()