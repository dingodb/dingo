# Tantivy Search

This library is designed to integrate [tantivy](https://github.com/quickwit-oss/tantivy/) into [DingoDB](https://github.com/dingodb/dingo-store).

## Development

All FFI (Foreign Function Interface) functions are exposed in [lib.rs](./src/lib.rs). Developers need to regenerate the header file after making any changes to the relevant code:

```bash
cbindgen . -o include/tantivy_search_cbindgen.h --config cbindgen.toml
cxxbridge src/lib.rs --header > include/tantivy_search_cxx.h
```

Developers can use gen-cxx-header.sh to do this too.

If developers do not add, delete, or modify the names of FFI functions, there is no need to execute the above command.

If developers do not have cbindgen or cxxbridge, install like this:

```bash
cargo install cbindgen
cargo install cxxbridge-cmd
```

## Work in progress

- [x] Added unit tests in C++ to test Rust FFI functions.
- [x] Add additional unit tests within Rust FFI functions.
- [ ] Refactor the `tantivy_search` code using object-oriented principles.

## How to build?

You can use `cargo` to build this library, use this command:

```bash
cargo build --release
```

If you need to test FFI function in C++, run:

```bash
mkdir build
cd build && cmake ..
make -j
```

You can use `vscode` or other compilers to make the build process more elegant.

## How to test?

Test in Rust:

```bash
cargo test
```

Here is an example to run unit test in C++:

```bash
cd build/tests/unit_test
./unit_test
```

## Credits
We give special thanks for these open-source projects, upon which we have developed:

- [Tantivy](https://github.com/quickwit-oss/tantivy/) - A library for full-text search.
- [tantivy-search](https://github.com/myscale/tantivy-search) - A library is designed to integrate tantivy into ClickHouse and MyScaleDB.
- [cang-jie](https://github.com/DCjanus/cang-jie) - A Chinese tokenizer for tantivy, based on jieba-rs.

