## Note

This library is designed for `ClickHouse`.

## Develop

All FFI (Foreign Function Interface) functions are exposed in `lib.rs`. Developers need to regenerate the header file after making any changes to the relevant code:

```bash
cbindgen . -o include/tantivy_search_cbindgen.h --config cbindgen.toml
cxxbridge src/lib.rs --header > include/tantivy_search_cxx.h
```

If developers do not add, delete, or modify the names of FFI functions, there is no need to execute the above command.

## Work in progress
- [x] Added unit tests in C++ to test Rust FFI functions.
- [ ] Add additional unit tests within Rust FFI functions.
- [ ] Refactor the `tantivy_search` code using object-oriented principles.


## How to build?
You can use `cargo` to build this library, use this command:
```bash
cargo build --release
```
Sometimes we need to test FFI function in CPP, and we need to run:
```bash
mkdir build
cd build && cmake ..
make -j
```
You can use `vscode` or other compilers to make the build process more elegant.

## How to test?
Here is an example to run unit test.
```bash
cd build/tests/unit_test
./unit_test
```