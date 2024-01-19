## Everything about this examples.

This example will be turned into a `CI/CD` in the future, which will be used to automatically test `QPS`, memory curves, index size, and the correctness of search results. 

It is based on the [wiki_560w](https://myscale-example-datasets.s3.amazonaws.com/wiki_560w.json) dataset, so please download this dataset to this directory in advance.

```bash
wget https://myscale-example-datasets.s3.amazonaws.com/wiki_560w.json
```

use valgrind
```
valgrind --read-var-info=yes --leak-check=full --show-leak-kinds=all -s ./query_benchmark --ps 1 --ebd=10 --efw=0 --it=100000 --ip=/home/mochix/tantivy_search_memory/cpp_pool1_no_cache/index_path --mpt=3 --sb=false> valgrind.txt 2>&1
```