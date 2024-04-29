#!/bin/bash
DIR=$( cd "$(dirname "${BASH_SOURCE[0]}")" && pwd);
echo $DIR

cd $DIR
cbindgen . -o include/tantivy_search_cbindgen.h --config cbindgen.toml
cxxbridge src/lib.rs --header > include/tantivy_search_cxx.h

