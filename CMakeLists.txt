cmake_minimum_required(VERSION 3.22)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

project(TANTIVY_LIB)
add_subdirectory(contrib ${CMAKE_CURRENT_BINARY_DIR}/contrib)

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(RUST_PROFILE release)
elseif(CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    set(RUST_PROFILE relwithdebinfo)
elseif(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(RUST_PROFILE dev)
else()
    set(RUST_PROFILE release)
endif()

corrosion_import_crate(
    MANIFEST_PATH "Cargo.toml"
    PROFILE ${RUST_PROFILE}
)

set(rust_lib "tantivy_search")
# -Wno-dollar-in-identifier-extension: cxx bridge complies names with '$'
# -Wno-unused-macros: unused CXXBRIDGE1_RUST_STRING
set(RUST_CXXFLAGS "-Wno-dollar-in-identifier-extension -Wno-unused-macros")
message(STATUS "Config cxx-bridge, use cxxflags: " ${RUST_CXXFLAGS})

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/.cargo/config.toml.in" "${CMAKE_CURRENT_SOURCE_DIR}/.cargo/config.toml" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/build.rs.in" "${CMAKE_CURRENT_SOURCE_DIR}/build.rs" @ONLY)


set(cxx_bridge_binary_folder ${CMAKE_BINARY_DIR}/cargo/build/${Rust_CARGO_TARGET_CACHED}/cxxbridge)
set(cxx_bridge_source_file "src/lib.rs")

set(common_header ${cxx_bridge_binary_folder}/rust/cxx.h)
# tantivy_search cxx header and source.
set(tantivy_search_header ${cxx_bridge_binary_folder}/${rust_lib}/${cxx_bridge_source_file}.h)
set(tantivy_search_source ${cxx_bridge_binary_folder}/${rust_lib}/${cxx_bridge_source_file}.cc)

# get include path
set(tantivy_search_include ${CMAKE_CURRENT_SOURCE_DIR}/include)

message(STATUS "❄️ -> common_header: ${tantivy_search_header}")
message(STATUS "❄️ -> tantivy_search header: ${tantivy_search_header}")
message(STATUS "❄️ -> tantivy_search source: ${tantivy_search_source}")
message(STATUS "❄️ -> tantivy_search include: ${tantivy_search_include}")

add_custom_command(
    OUTPUT
    ${common_header}
    ${tantivy_search_header}
    ${tantivy_search_source}
    COMMAND
    DEPENDS ${rust_lib}-static
    COMMENT "Fixing cmake to find source files"
)


add_library(${rust_lib}_cxxbridge
    ${common_header}
    ${skim_header}
    ${skim_source}
    ${tantivy_search_header}
    ${tantivy_search_source}
)
# cxx bridge compiles such bindings
set_target_properties(${rust_lib}_cxxbridge PROPERTIES COMPILE_FLAGS "${CXXBRIDGE_CXXFLAGS}")
set_target_properties(${rust_lib}_cxxbridge PROPERTIES LINKER_LANGUAGE CXX)

target_include_directories(${rust_lib}_cxxbridge
    PUBLIC
    ${tantivy_search_include}
)

# Create total target with alias with given namespace
add_library(${rust_lib}-total INTERFACE)
target_link_libraries(${rust_lib}-total
    INTERFACE
    ${rust_lib}_cxxbridge
    ${rust_lib}
)

# for end-user to link into project
add_library(rust::${rust_lib} ALIAS ${rust_lib}-total)

add_subdirectory(benchmark ${CMAKE_CURRENT_BINARY_DIR}/benchmark)
add_subdirectory(tests ${CMAKE_CURRENT_BINARY_DIR}/tests)
add_subdirectory(examples ${CMAKE_CURRENT_BINARY_DIR}/examples)

