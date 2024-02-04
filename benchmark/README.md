# Benchmark

## How to Run the Benchmark?

If this is your first time running this benchmark, ensure you do not skip building the tantivy index.
Before running the benchmark, relevant datasets need to be downloaded.
Please refer to this [scripts](../scripts/download_datasets.sh) for download instructions.

```bash
./tantivy_search_benchmark --qtp=query_terms.json --dp=wiki_560w.json --ip=tantivy_index_path --sbi=false
```

Here is the result sample:

```text
Building index for WikiSkipIndexSearchBenchmark
Build finished.
2024-01-29T18:43:06+08:00
Running ./tantivy_search_benchmark
Run on (16 X 4933.89 MHz CPU s)
CPU Caches:
  L1 Data 32 KiB (x8)
  L1 Instruction 32 KiB (x8)
  L2 Unified 512 KiB (x8)
  L3 Unified 16384 KiB (x1)
Load Average: 6.13, 4.34, 2.54
***WARNING*** CPU scaling is enabled, the benchmark real time measurements may be noisy and will incur extra overhead.
-------------------------------------------------------------------------------------------------------------
Benchmark                                                                   Time             CPU   Iterations
-------------------------------------------------------------------------------------------------------------
WikiSkipIndexSearchBenchmark/SkipGranule1K/iterations:4/threads:16        480 ms         6978 ms           64
WikiBm25SearchBenchmark/Top10Bm25Search1K/iterations:4/threads:16         127 ms         42.1 ms           64
WikiBm25SearchBenchmark/Top100Bm25Search1K/iterations:4/threads:16        260 ms         92.8 ms           64
```

## How to Add a New Benchmark?

Step 1: Create a header file.

Step 2: Define and register your benchmark using Google Benchmark.

Step 3: Include your new header file in `main.cpp`.
