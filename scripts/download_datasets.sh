#!/bin/bash

SCRIPT_DIR=$(dirname "$0")

wget -P "$SCRIPT_DIR" https://mqdb-release-1253802058.cos.ap-beijing.myqcloud.com/datasets/wiki_560w.json
wget -P "$SCRIPT_DIR" https://mqdb-release-1253802058.cos.ap-beijing.myqcloud.com/datasets/query_terms.json