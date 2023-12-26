## Note

This library is designed for `ClickHouse`.

## Build

All FFI (Foreign Function Interface) functions are exposed in `lib.rs`. Developers need to regenerate the header file after making any changes to the relevant code:

```bash
cbindgen . -o include/tantivy_search.h --config cbindgen.toml 
```

If developers do not add, delete, or modify the names of FFI functions, there is no need to execute the above command.