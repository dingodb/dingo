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

using json = nlohmann::json;

using namespace std;
namespace fs = std::filesystem;


class Wiki560wBM25SearchBenchmark : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) override {
        tantivy_load_index(WikiDatasetLoader::getInstance().getIndexDirectory());
    }

    void TearDown(const ::benchmark::State& state) override {
        tantivy_reader_free(WikiDatasetLoader::getInstance().getIndexDirectory());
    }

    void PerformSearch(benchmark::State& state, size_t topK) {
        // Search for all given terms.
        auto query_terms = WikiDatasetLoader::getInstance().loadQueryTerms();
        std::vector<size_t> all_indexes(query_terms.size());
        std::iota(all_indexes.begin(), all_indexes.end(), 0); // generate indices from 0 to query_terms.size()
        SearchImpl(state, topK, all_indexes);
    }

    void PerformRandomSearch(benchmark::State& state, size_t topK, size_t queryCount) {
        // Only random choose some terms to search.
        auto query_terms = WikiDatasetLoader::getInstance().loadQueryTerms();
        auto random_indexes = WikiDatasetLoader::getInstance().generateRandomArray(queryCount, 0, query_terms.size());
        SearchImpl(state, topK, random_indexes);
    }
private:
    void SearchImpl(benchmark::State& state, size_t topK, const std::vector<size_t>& indexes) {
        auto index_directory = WikiDatasetLoader::getInstance().getIndexDirectory();
        auto query_terms = WikiDatasetLoader::getInstance().loadQueryTerms();

        // `queries` records the total number of bm25 queries.
        uint64_t queries = 0;
        for (auto _ : state) {
            for (auto i : indexes) {
                tantivy_bm25_search(index_directory, query_terms[i], topK, false);
            }
            queries += indexes.size();
        }

        // Record custom counters
        state.counters["Queries"] = queries;
        state.counters["QPS(total)"] = benchmark::Counter(queries, benchmark::Counter::kIsRate);
        state.counters["QPS(avgThreads)"] = benchmark::Counter(queries, benchmark::Counter::kAvgThreadsRate);
    }
};

#define WIKI_560W_RANDOM_BM25_SEARCH_BENCHMARK_REGISTER(topK, queryCount) \
    BENCHMARK_DEFINE_F(Wiki560wBM25SearchBenchmark, BM25_RandomQuery_##queryCount##_Terms_Wiki560w_Topk_##topK)(benchmark::State& state) \
    { \
        PerformRandomSearch(state, topK, queryCount); \
    } \
    BENCHMARK_REGISTER_F(Wiki560wBM25SearchBenchmark, BM25_RandomQuery_##queryCount##_Terms_Wiki560w_Topk_##topK) \
        ->Threads(1) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(Wiki560wBM25SearchBenchmark, BM25_RandomQuery_##queryCount##_Terms_Wiki560w_Topk_##topK) \
        ->Threads(2) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(Wiki560wBM25SearchBenchmark, BM25_RandomQuery_##queryCount##_Terms_Wiki560w_Topk_##topK) \
        ->Threads(4) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(Wiki560wBM25SearchBenchmark, BM25_RandomQuery_##queryCount##_Terms_Wiki560w_Topk_##topK) \
        ->Threads(8) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond);

#define WIKI_560W_NORMAL_BM25_SEARCH_BENCHMARK_REGISTER(topK) \
    BENCHMARK_DEFINE_F(Wiki560wBM25SearchBenchmark, BM25_NormalQuery_##queryCount##_Terms_Wiki560w_Topk_##topK)(benchmark::State& state) \
    { \
        PerformSearch(state, topK); \
    } \
    BENCHMARK_REGISTER_F(Wiki560wBM25SearchBenchmark, BM25_NormalQuery_##queryCount##_Terms_Wiki560w_Topk_##topK) \
        ->Threads(1) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(Wiki560wBM25SearchBenchmark, BM25_NormalQuery_##queryCount##_Terms_Wiki560w_Topk_##topK) \
        ->Threads(2) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(Wiki560wBM25SearchBenchmark, BM25_NormalQuery_##queryCount##_Terms_Wiki560w_Topk_##topK) \
        ->Threads(4) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(Wiki560wBM25SearchBenchmark, BM25_NormalQuery_##queryCount##_Terms_Wiki560w_Topk_##topK) \
        ->Threads(8) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond);


WIKI_560W_RANDOM_BM25_SEARCH_BENCHMARK_REGISTER(10, 1000);
WIKI_560W_RANDOM_BM25_SEARCH_BENCHMARK_REGISTER(100, 1000);
WIKI_560W_RANDOM_BM25_SEARCH_BENCHMARK_REGISTER(200, 1000);
WIKI_560W_RANDOM_BM25_SEARCH_BENCHMARK_REGISTER(500, 1000);

// WIKI_560W_NORMAL_BM25_SEARCH_BENCHMARK_REGISTER(10);
// WIKI_560W_NORMAL_BM25_SEARCH_BENCHMARK_REGISTER(100);
// WIKI_560W_NORMAL_BM25_SEARCH_BENCHMARK_REGISTER(200);
// WIKI_560W_NORMAL_BM25_SEARCH_BENCHMARK_REGISTER(500);