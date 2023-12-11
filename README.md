## Note

本 library 用于 Clickhouse

## build

所有的 FFI 函数会放在 lib.rs 内对外暴露。开发人员在修改相关的代码之后需要重新生成头文件:
```bash
cbindgen . -o include/tantivy_search.h --config cbindgen.toml 
```
如果开发人员没有对 FFI 函数进行新增/删除/修改 FFI 名称, 则无需执行上述命令