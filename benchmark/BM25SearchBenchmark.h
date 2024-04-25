#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <fstream>
#include <nlohmann/json.hpp>
#include <ThreadPool.h>
#include <tantivy_search.h>
#include <random>
#include <sstream>
#include <iomanip>
#include <unistd.h>
#include <boost/program_options.hpp>
#include <benchmark/benchmark.h>
#include <WikiDatasetLoader.h>
#include <filesystem>
#include <BenchmarkConfig.h>

using json = nlohmann::json;

using namespace std;
namespace fs = std::filesystem;


class BM25 : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) override {
        ffi_load_index_reader(QueryTerms::TANTIVY_INDEX_FILES_PATH);
    }

    void TearDown(const ::benchmark::State& state) override {
        ffi_free_index_reader(QueryTerms::TANTIVY_INDEX_FILES_PATH);
    }

    void BM25Search(benchmark::State& state, size_t topK) {
        // `queries` records the total number of bm25 queries.
        uint64_t queries = 0;
        for (auto _ : state) {
            for (size_t i = 0; i < 1000; i++)
            {
                ffi_bm25_search(
                    QueryTerms::TANTIVY_INDEX_FILES_PATH,
                    QueryTerms::QUERY_SENTENCE_WITHIN_5_TERMS[ (i+queries)%QueryTerms::QUERY_SENTENCE_WITHIN_5_TERMS.size() ],
                    topK,
                    {},
                    false
                );
            }
            queries += 1000;
        }
        state.counters["QPS"] = benchmark::Counter(queries, benchmark::Counter::kIsRate);
        state.counters["QPS(avgThreads)"] = benchmark::Counter(queries, benchmark::Counter::kAvgThreadsRate);
    }

    void BM25SearchWithFilter(benchmark::State& state, size_t topK) {
        // `queries` records the total number of bm25 queries.
        uint64_t queries = 0;
        for (auto _ : state) {
            for (size_t i = 0; i < 1000; i++)
            {
                ffi_bm25_search(
                    QueryTerms::TANTIVY_INDEX_FILES_PATH,
                    QueryTerms::QUERY_SENTENCE_WITHIN_5_TERMS[ (i+queries)%QueryTerms::QUERY_SENTENCE_WITHIN_5_TERMS.size() ],
                    topK,
                    QueryTerms::ALIVED_U8_BITMAP[ (i+queries)%QueryTerms::ALIVED_U8_BITMAP.size() ],
                    true
                );
            }
            queries += 1000;
        }
        state.counters["QPS"] = benchmark::Counter(queries, benchmark::Counter::kIsRate);
        state.counters["QPS(avgThreads)"] = benchmark::Counter(queries, benchmark::Counter::kAvgThreadsRate);
    }
};

#define WIKI_560W_BM25_SEARCH_BenchmarkRegister(topK) \
    BENCHMARK_DEFINE_F(BM25, wiki5m_search_1k_top_##topK)(benchmark::State& state) \
    { \
        BM25Search(state, topK); \
    } \
    BENCHMARK_REGISTER_F(BM25, wiki5m_search_1k_top_##topK) \
        ->Threads(1) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(BM25, wiki5m_search_1k_top_##topK) \
        ->Threads(8) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond);

#define WIKI_560W_BM25_SEARCH_FILTER_BenchmarkRegister(topK) \
    BENCHMARK_DEFINE_F(BM25, wiki5m_u8_filter_1k_top_##topK)(benchmark::State& state) \
    { \
        BM25SearchWithFilter(state, topK); \
    } \
    BENCHMARK_REGISTER_F(BM25, wiki5m_u8_filter_1k_top_##topK) \
        ->Threads(1) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(BM25, wiki5m_u8_filter_1k_top_##topK) \
        ->Threads(8) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond);


WIKI_560W_BM25_SEARCH_BenchmarkRegister(10);
WIKI_560W_BM25_SEARCH_BenchmarkRegister(500);
WIKI_560W_BM25_SEARCH_FILTER_BenchmarkRegister(10);
WIKI_560W_BM25_SEARCH_FILTER_BenchmarkRegister(500);