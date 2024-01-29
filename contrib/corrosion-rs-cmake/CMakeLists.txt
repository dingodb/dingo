message(STATUS "Checking Rust toolchain for current target")

if(CMAKE_TOOLCHAIN_FILE MATCHES "linux/toolchain-x86_64")
    set(Rust_CARGO_TARGET "x86_64-unknown-linux-gnu")
endif()

if(CMAKE_TOOLCHAIN_FILE MATCHES "linux/toolchain-aarch64")
    set(Rust_CARGO_TARGET "aarch64-unknown-linux-gnu")
endif()

if((CMAKE_TOOLCHAIN_FILE MATCHES "darwin") AND (CMAKE_TOOLCHAIN_FILE MATCHES "x86_64"))
    set(Rust_CARGO_TARGET "x86_64-apple-darwin")
endif()

if((CMAKE_TOOLCHAIN_FILE MATCHES "freebsd") AND (CMAKE_TOOLCHAIN_FILE MATCHES "x86_64"))
    set(Rust_CARGO_TARGET "x86_64-unknown-freebsd")
endif()

if(CMAKE_TOOLCHAIN_FILE MATCHES "ppc64le")
    set(Rust_CARGO_TARGET "powerpc64le-unknown-linux-gnu")
endif()

message(STATUS "Switched Rust target to ${Rust_CARGO_TARGET}")

# FindRust.cmake
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../corrosion-rs/cmake")
# Define function corrosion_import_crate()
include ("${CMAKE_CURRENT_SOURCE_DIR}/../corrosion-rs/cmake/Corrosion.cmake")
