#include <BenchmarkConfig.h>
#include <ThreadPool.h>
#include <WikiDatasetLoader.h>
#include <benchmark/benchmark.h>
#include <tantivy_search.h>
#include <unistd.h>

#include <atomic>
#include <bitset>
#include <boost/program_options.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <random>
#include <roaring.hh>
#include <sstream>
#include <thread>
#include <vector>

using json = nlohmann::json;

using namespace std;
namespace fs = std::filesystem;

class SkipIndex : public benchmark::Fixture {
 public:
  void SetUp(const ::benchmark::State& state) override {
    ffi_load_index_reader(QueryTerms::TANTIVY_INDEX_FILES_PATH);
  }

  void TearDown(const ::benchmark::State& state) override {
    ffi_free_index_reader(QueryTerms::TANTIVY_INDEX_FILES_PATH);
  }

  void QueryOneTermBitmap(benchmark::State& state, int index_granularity,
                          int total_rows) {
    uint64_t queries = 0;
    for (auto _ : state) {
      for (size_t i = 0; i < 1000; i++) {
        // simulate for a table search.
        rust::cxxbridge1::Vec<uint8_t> res = ffi_query_term_bitmap(
            QueryTerms::TANTIVY_INDEX_FILES_PATH, "text",
            QueryTerms::QUERY_TERMS[(i + queries) %
                                    QueryTerms::QUERY_TERMS.size()]);
        roaring::Roaring searched = convertU8BitmapToRoaring(res);
        for (size_t row_id = 0; row_id < total_rows;
             row_id += index_granularity) {
          roaring::Roaring granule;
          granule.addRangeClosed(row_id, row_id + index_granularity);
          bool can_skip = (searched & granule).cardinality() == 0;
        }
      }
      queries += 1000;
    }
    state.counters["QPS"] =
        benchmark::Counter(queries, benchmark::Counter::kIsRate);
    state.counters["QPS(avgThreads)"] =
        benchmark::Counter(queries, benchmark::Counter::kAvgThreadsRate);
  }

  void Query5TermsBitmap(benchmark::State& state, int index_granularity,
                         int total_rows) {
    uint64_t queries = 0;
    for (auto _ : state) {
      for (size_t i = 0; i < 1000; i++) {
        // simulate for a table search.
        rust::cxxbridge1::Vec<uint8_t> res = ffi_query_terms_bitmap(
            QueryTerms::TANTIVY_INDEX_FILES_PATH, "text",
            QueryTerms::QUERY_5TERMS_SETS
                [(i + queries) % QueryTerms::QUERY_5TERMS_SETS.size()]);
        roaring::Roaring searched = convertU8BitmapToRoaring(res);
        for (size_t row_id = 0; row_id < total_rows;
             row_id += index_granularity) {
          roaring::Roaring granule;
          granule.addRangeClosed(row_id, row_id + index_granularity);
          bool can_skip = (searched & granule).cardinality() == 0;
        }
      }
      queries += 1000;
    }
    state.counters["QPS"] =
        benchmark::Counter(queries, benchmark::Counter::kIsRate);
    state.counters["QPS(avgThreads)"] =
        benchmark::Counter(queries, benchmark::Counter::kAvgThreadsRate);
  }

  void Query10TermsBitmap(benchmark::State& state, int index_granularity,
                          int total_rows) {
    uint64_t queries = 0;
    for (auto _ : state) {
      for (size_t i = 0; i < 1000; i++) {
        // simulate for a table search.
        rust::cxxbridge1::Vec<uint8_t> res = ffi_query_terms_bitmap(
            QueryTerms::TANTIVY_INDEX_FILES_PATH, "text",
            QueryTerms::QUERY_10TERMS_SETS
                [(i + queries) % QueryTerms::QUERY_10TERMS_SETS.size()]);
        roaring::Roaring searched = convertU8BitmapToRoaring(res);
        for (size_t row_id = 0; row_id < total_rows;
             row_id += index_granularity) {
          roaring::Roaring granule;
          granule.addRangeClosed(row_id, row_id + index_granularity);
          bool can_skip = (searched & granule).cardinality() == 0;
        }
      }
      queries += 1000;
    }
    state.counters["QPS"] =
        benchmark::Counter(queries, benchmark::Counter::kIsRate);
    state.counters["QPS(avgThreads)"] =
        benchmark::Counter(queries, benchmark::Counter::kAvgThreadsRate);
  }

  void QuerySentenceWith5TermsBitmap(benchmark::State& state,
                                     int index_granularity, int total_rows) {
    uint64_t queries = 0;
    for (auto _ : state) {
      for (size_t i = 0; i < 1000; i++) {
        // simulate for a table search.
        rust::cxxbridge1::Vec<uint8_t> res = ffi_query_sentence_bitmap(
            QueryTerms::TANTIVY_INDEX_FILES_PATH, "text",
            QueryTerms::QUERY_SENTENCE_WITHIN_5_TERMS
                [(i + queries) %
                 QueryTerms::QUERY_SENTENCE_WITHIN_5_TERMS.size()]);
        roaring::Roaring searched = convertU8BitmapToRoaring(res);
        for (size_t row_id = 0; row_id < total_rows;
             row_id += index_granularity) {
          roaring::Roaring granule;
          granule.addRangeClosed(row_id, row_id + index_granularity);
          bool can_skip = (searched & granule).cardinality() == 0;
        }
      }
      queries += 1000;
    }
    state.counters["QPS"] =
        benchmark::Counter(queries, benchmark::Counter::kIsRate);
    state.counters["QPS(avgThreads)"] =
        benchmark::Counter(queries, benchmark::Counter::kAvgThreadsRate);
  }

  void QuerySentenceWith10TermsBitmap(benchmark::State& state,
                                      int index_granularity, int total_rows) {
    uint64_t queries = 0;
    for (auto _ : state) {
      for (size_t i = 0; i < 1000; i++) {
        // simulate for a table search.
        rust::cxxbridge1::Vec<uint8_t> res = ffi_query_sentence_bitmap(
            QueryTerms::TANTIVY_INDEX_FILES_PATH, "text",
            QueryTerms::QUERY_SENTENCE_WITHIN_10_TERMS
                [(i + queries) %
                 QueryTerms::QUERY_SENTENCE_WITHIN_10_TERMS.size()]);
        roaring::Roaring searched = convertU8BitmapToRoaring(res);
        for (size_t row_id = 0; row_id < total_rows;
             row_id += index_granularity) {
          roaring::Roaring granule;
          granule.addRangeClosed(row_id, row_id + index_granularity);
          bool can_skip = (searched & granule).cardinality() == 0;
        }
      }
      queries += 1000;
    }
    state.counters["QPS"] =
        benchmark::Counter(queries, benchmark::Counter::kIsRate);
    state.counters["QPS(avgThreads)"] =
        benchmark::Counter(queries, benchmark::Counter::kAvgThreadsRate);
  }

  void QueryPatternBitmap(benchmark::State& state, int index_granularity,
                          int total_rows) {
    uint64_t queries = 0;
    for (auto _ : state) {
      for (size_t i = 0; i < 1000; i++) {
        // simulate for a table search.
        rust::cxxbridge1::Vec<uint8_t> res = ffi_regex_term_bitmap(
            QueryTerms::TANTIVY_INDEX_FILES_PATH, "text",
            QueryTerms::QUERY_PATTERNS[(i + queries) %
                                       QueryTerms::QUERY_PATTERNS.size()]);
        roaring::Roaring searched = convertU8BitmapToRoaring(res);
        for (size_t row_id = 0; row_id < total_rows;
             row_id += index_granularity) {
          roaring::Roaring granule;
          granule.addRangeClosed(row_id, row_id + index_granularity);
          bool can_skip = (searched & granule).cardinality() == 0;
        }
      }
      queries += 1000;
    }
    state.counters["QPS"] =
        benchmark::Counter(queries, benchmark::Counter::kIsRate);
    state.counters["QPS(avgThreads)"] =
        benchmark::Counter(queries, benchmark::Counter::kAvgThreadsRate);
  }

  roaring::Roaring convertU8BitmapToRoaring(
      rust::cxxbridge1::Vec<uint8_t>& u8_bitmap) {
    roaring::Roaring roaringBitmap;
    // caculate roaring bitmap. (u32)
    for (size_t i = 0; i < u8_bitmap.size(); i++) {
      if (u8_bitmap[i] == 0) {
        continue;
      }
      std::bitset<8> temp(u8_bitmap[i]);
      size_t bit = i * 8;
      for (size_t k = 0; k < temp.size(); k++) {
        if (temp[k]) {
          roaringBitmap.add(bit + k);
        }
      }
    }
    return roaringBitmap;
  }
};

#define WIKI_560W_SKIPINDEX_QUERY_ONE_TERM_1K_BenchmarkRegister(granuleSize, \
                                                                totalRows)   \
  BENCHMARK_DEFINE_F(SkipIndex, wiki5m_one_term_1k_ig_##granuleSize)         \
  (benchmark::State & state) {                                               \
    QueryOneTermBitmap(state, granuleSize, totalRows);                       \
  }                                                                          \
  BENCHMARK_REGISTER_F(SkipIndex, wiki5m_one_term_1k_ig_##granuleSize)       \
      ->Threads(1)                                                           \
      ->Iterations(4)                                                        \
      ->Unit(benchmark::kMillisecond);                                       \
  BENCHMARK_REGISTER_F(SkipIndex, wiki5m_one_term_1k_ig_##granuleSize)       \
      ->Threads(8)                                                           \
      ->Iterations(4)                                                        \
      ->Unit(benchmark::kMillisecond);

#define WIKI_560W_SKIPINDEX_QUERY_5TERMS_1K_BenchmarkRegister(granuleSize, \
                                                              totalRows)   \
  BENCHMARK_DEFINE_F(SkipIndex, wiki5m_5_terms_1k_ig_##granuleSize)        \
  (benchmark::State & state) {                                             \
    Query5TermsBitmap(state, granuleSize, totalRows);                      \
  }                                                                        \
  BENCHMARK_REGISTER_F(SkipIndex, wiki5m_5_terms_1k_ig_##granuleSize)      \
      ->Threads(1)                                                         \
      ->Iterations(4)                                                      \
      ->Unit(benchmark::kMillisecond);                                     \
  BENCHMARK_REGISTER_F(SkipIndex, wiki5m_5_terms_1k_ig_##granuleSize)      \
      ->Threads(8)                                                         \
      ->Iterations(4)                                                      \
      ->Unit(benchmark::kMillisecond);

#define WIKI_560W_SKIPINDEX_QUERY_10TERMS_1K_BenchmarkRegister(granuleSize, \
                                                               totalRows)   \
  BENCHMARK_DEFINE_F(SkipIndex, wiki5m_10_terms_1k_ig_##granuleSize)        \
  (benchmark::State & state) {                                              \
    Query10TermsBitmap(state, granuleSize, totalRows);                      \
  }                                                                         \
  BENCHMARK_REGISTER_F(SkipIndex, wiki5m_10_terms_1k_ig_##granuleSize)      \
      ->Threads(1)                                                          \
      ->Iterations(4)                                                       \
      ->Unit(benchmark::kMillisecond);                                      \
  BENCHMARK_REGISTER_F(SkipIndex, wiki5m_10_terms_1k_ig_##granuleSize)      \
      ->Threads(8)                                                          \
      ->Iterations(4)                                                       \
      ->Unit(benchmark::kMillisecond);

#define WIKI_560W_SKIPINDEX_QUERY_SENTENCE_WITHIN_5TERMS_1K_BenchmarkRegister( \
    granuleSize, totalRows)                                                    \
  BENCHMARK_DEFINE_F(SkipIndex, wiki5m_sentence_5_terms_1k_##granuleSize)      \
  (benchmark::State & state) {                                                 \
    QuerySentenceWith5TermsBitmap(state, granuleSize, totalRows);              \
  }                                                                            \
  BENCHMARK_REGISTER_F(SkipIndex, wiki5m_sentence_5_terms_1k_##granuleSize)    \
      ->Threads(1)                                                             \
      ->Iterations(4)                                                          \
      ->Unit(benchmark::kMillisecond);                                         \
  BENCHMARK_REGISTER_F(SkipIndex, wiki5m_sentence_5_terms_1k_##granuleSize)    \
      ->Threads(8)                                                             \
      ->Iterations(4)                                                          \
      ->Unit(benchmark::kMillisecond);

#define WIKI_560W_SKIPINDEX_QUERY_SENTENCE_WITHIN_10TERMS_1K_BenchmarkRegister( \
    granuleSize, totalRows)                                                     \
  BENCHMARK_DEFINE_F(SkipIndex, wiki5m_sentence_10_terms_1k_##granuleSize)      \
  (benchmark::State & state) {                                                  \
    QuerySentenceWith10TermsBitmap(state, granuleSize, totalRows);              \
  }                                                                             \
  BENCHMARK_REGISTER_F(SkipIndex, wiki5m_sentence_10_terms_1k_##granuleSize)    \
      ->Threads(1)                                                              \
      ->Iterations(4)                                                           \
      ->Unit(benchmark::kMillisecond);                                          \
  BENCHMARK_REGISTER_F(SkipIndex, wiki5m_sentence_10_terms_1k_##granuleSize)    \
      ->Threads(8)                                                              \
      ->Iterations(4)                                                           \
      ->Unit(benchmark::kMillisecond);

#define WIKI_560W_SKIPINDEX_REGEX_QUERY_1K_BenchmarkRegister(granuleSize, \
                                                             totalRows)   \
  BENCHMARK_DEFINE_F(SkipIndex, wiki5m_regex_1k_##granuleSize)            \
  (benchmark::State & state) {                                            \
    QueryPatternBitmap(state, granuleSize, totalRows);                    \
  }                                                                       \
  BENCHMARK_REGISTER_F(SkipIndex, wiki5m_regex_1k_##granuleSize)          \
      ->Threads(1)                                                        \
      ->Iterations(4)                                                     \
      ->Unit(benchmark::kMillisecond);                                    \
  BENCHMARK_REGISTER_F(SkipIndex, wiki5m_regex_1k_##granuleSize)          \
      ->Threads(8)                                                        \
      ->Iterations(4)                                                     \
      ->Unit(benchmark::kMillisecond);

WIKI_560W_SKIPINDEX_QUERY_ONE_TERM_1K_BenchmarkRegister(
    128,
    5000000) WIKI_560W_SKIPINDEX_QUERY_ONE_TERM_1K_BenchmarkRegister(8192,
                                                                     5000000)
    WIKI_560W_SKIPINDEX_QUERY_5TERMS_1K_BenchmarkRegister(
        128,
        5000000) WIKI_560W_SKIPINDEX_QUERY_5TERMS_1K_BenchmarkRegister(8192,
                                                                       5000000)
        WIKI_560W_SKIPINDEX_QUERY_10TERMS_1K_BenchmarkRegister(
            128,
            5000000) WIKI_560W_SKIPINDEX_QUERY_10TERMS_1K_BenchmarkRegister(8192,
                                                                            5000000)
            WIKI_560W_SKIPINDEX_QUERY_SENTENCE_WITHIN_5TERMS_1K_BenchmarkRegister(
                128, 5000000)
                WIKI_560W_SKIPINDEX_QUERY_SENTENCE_WITHIN_5TERMS_1K_BenchmarkRegister(
                    8192, 5000000)
                    WIKI_560W_SKIPINDEX_QUERY_SENTENCE_WITHIN_10TERMS_1K_BenchmarkRegister(
                        128, 5000000)
                        WIKI_560W_SKIPINDEX_QUERY_SENTENCE_WITHIN_10TERMS_1K_BenchmarkRegister(
                            8192, 5000000)
                            WIKI_560W_SKIPINDEX_REGEX_QUERY_1K_BenchmarkRegister(
                                128, 5000000)
                                WIKI_560W_SKIPINDEX_REGEX_QUERY_1K_BenchmarkRegister(
                                    8192, 5000000)
