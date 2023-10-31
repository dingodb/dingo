# tantivy 构建方式
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_CMD cargo build)
    set(TARGET_DIR "debug")
else ()
    set(CARGO_CMD cargo build --release)
    set(TARGET_DIR "release")
endif ()

# tantivy 静态库
set(LIBRARY_TANTIVY_SEARCH_FILE ${CMAKE_CURRENT_BINARY_DIR}/target/${TARGET_DIR}/libtantivy_search.a)

# 仅构建的时候执行
add_custom_target(rs ALL
COMMENT "Compiling TANTIVY_SEARCH module"
COMMAND CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}/target ${CARGO_CMD}
WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})


# 尝试查找 tantivy 静态库文件, 如果在构建目录中不存在, 则不会报错
find_library(LIBRARY_TANTIVY_SEARCH NAMES LIBRARY_TANTIVY_SEARCH_FILE tantivy_search PATHS ${CMAKE_CURRENT_BINARY_DIR}/target/${TARGET_DIR} NO_DEFAULT_PATH)

# 判断静态库 tantivy 是否存在
if(LIBRARY_TANTIVY_SEARCH)
  message(STATUS "tantivy library path is " ${LIBRARY_TANTIVY_SEARCH})
else()
  set(LIBRARY_TANTIVY_SEARCH "LIBRARY_TANTIVY_SEARCH-NOTFOUND")
  message(STATUS "Library tantivy_search will be researched for at build time.")
endif()


find_path (INCLUDE_TANTIVY_SEARCH NAMES tantivy_search.h  PATHS ${CMAKE_CURRENT_SOURCE_DIR}/include)


if (LIBRARY_TANTIVY_SEARCH AND INCLUDE_TANTIVY_SEARCH)
  set(CMAKE_REQUIRED_LIBRARIES ${LIBRARY_TANTIVY_SEARCH})
  set(CMAKE_REQUIRED_INCLUDES ${INCLUDE_TANTIVY_SEARCH})
  # 定义 tantivy 新的库目标
  add_library (tantivy_search STATIC IMPORTED)
  # 定义 tantivy 库目标的完整路径
  set_property (TARGET tantivy_search PROPERTY IMPORTED_LOCATION  ${LIBRARY_TANTIVY_SEARCH})
  # 定义其它目标链接到这个库时应当包含哪些目录
  set_property (TARGET tantivy_search PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${INCLUDE_TANTIVY_SEARCH})
  message (STATUS "Using tantivysearch: ${INCLUDE_TANTIVY_SEARCH} : ${LIBRARY_TANTIVY_SEARCH}")
else()
  message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find tantivy")
endif()