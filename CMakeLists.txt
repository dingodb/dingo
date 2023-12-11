cmake_minimum_required(VERSION 3.22)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

project(TANTIVY_LIB)

# tantivy 构建方式
# if (CMAKE_BUILD_TYPE STREQUAL "Debug")
#     set(CARGO_CMD cargo update -p tantivy && cargo build)
#     set(TARGET_DIR "debug")
# else ()
set(CARGO_CMD cargo build --release)
set(TARGET_DIR "release")
# endif ()

# tantivy 静态库路径
set(LIB_SEARCH_PATH ${CMAKE_CURRENT_BINARY_DIR}/target/${TARGET_DIR})
set(LIB_INCLUDE ${CMAKE_CURRENT_SOURCE_DIR}/include)

# 自定义构建脚本, 后续比较繁琐的话可以单独放到一个 .sh 文件
add_custom_target(tantivy_library_build_target ALL
  COMMENT "Compiling TANTIVY_SEARCH module"
  COMMAND CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}/target ${CARGO_CMD}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

# 尝试查找 tantivy 静态库文件, 如果在构建目录中不存在, 则不会报错
find_library(LIB_FILE NAMES tantivy_search PATHS ${LIB_SEARCH_PATH} NO_DEFAULT_PATH)

# 判断静态库 tantivy 是否存在
if(LIB_FILE)
  message(STATUS "Tantivy libtantivy_search.a path is " ${LIB_FILE})
else()
  set(LIB_FILE ${LIB_SEARCH_PATH}/libtantivy_search.a)
  # 临时空目录和文件
  file(MAKE_DIRECTORY ${LIB_SEARCH_PATH})
  file(TOUCH ${LIB_FILE})
  message(STATUS "Tantivy libtantivy_search.a will be rebuild during build time.")
endif()


if (LIB_FILE AND LIB_INCLUDE)
  set(CMAKE_REQUIRED_LIBRARIES ${LIB_FILE})
  set(CMAKE_REQUIRED_INCLUDES ${LIB_INCLUDE})
  add_library (tantivy_search INTERFACE)
  # 提供 libtantivy.a 静态的完整路径
  set_property (TARGET tantivy_search PROPERTY INTERFACE_LINK_LIBRARIES  ${LIB_FILE})
  # 其它 target 链接到这个库时 include 目录路径
  set_property (TARGET tantivy_search PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${LIB_INCLUDE})
  message (STATUS "Tantivy rust library is ready")
else()
  message (STATUS "Tantivy rust library doesn't build during the configure stage.")
endif()


add_subdirectory(contrib)
add_subdirectory(examples)