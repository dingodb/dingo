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


class BM25 : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) override {
        InitializeResources();
        tantivy_load_index(this->indexDirectory);
    }

    void TearDown(const ::benchmark::State& state) override {
        tantivy_reader_free(this->indexDirectory);
    }

    void PerformSearch(benchmark::State& state, size_t topK) {
        // Search for all given terms.
        SearchImpl(state, topK, this->allIndices);
    }

    void PerformRandomSearch(benchmark::State& state, size_t topK, size_t queryCount) {
        // Only random choose some terms to search.
        auto randomIndexes = WikiDatasetLoader::getInstance().generateRandomArray(queryCount, 0, this->queryTerms.size());
        SearchImpl(state, topK, randomIndexes);
    }
private:
    mutex resourceMutex;
    vector<string> queryTerms;
    vector<size_t> allIndices;
    string indexDirectory;

    void InitializeResources(){
        lock_guard<mutex> lock(resourceMutex);
        this->queryTerms = WikiDatasetLoader::getInstance().loadQueryTerms();
        this->allIndices.resize(this->queryTerms.size());
        iota(this->allIndices.begin(), this->allIndices.end(), 0);
        this->indexDirectory = WikiDatasetLoader::getInstance().getIndexDirectory(); 
    }

    void SearchImpl(benchmark::State& state, size_t topK, const std::vector<size_t>& indexes) {
        // `queries` records the total number of bm25 queries.
        uint64_t queries = 0;
        for (auto _ : state) {
            for (auto i : indexes) {
                tantivy_bm25_search(this->indexDirectory, this->queryTerms[i], topK, false);
            }
            queries += indexes.size();
        }

        state.counters["QPS"] = benchmark::Counter(queries, benchmark::Counter::kIsRate);
        state.counters["QPS(avgThreads)"] = benchmark::Counter(queries, benchmark::Counter::kAvgThreadsRate);
    }
};

#define WIKI_560W_RANDOM_BM25_SEARCH_BENCHMARK_REGISTER(topK, queryCount) \
    BENCHMARK_DEFINE_F(BM25, rand_##queryCount##_k_##topK)(benchmark::State& state) \
    { \
        PerformRandomSearch(state, topK, queryCount); \
    } \
    BENCHMARK_REGISTER_F(BM25, rand_##queryCount##_k_##topK) \
        ->Threads(1) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(BM25, rand_##queryCount##_k_##topK) \
        ->Threads(2) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(BM25, rand_##queryCount##_k_##topK) \
        ->Threads(4) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(BM25, rand_##queryCount##_k_##topK) \
        ->Threads(8) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond);

#define WIKI_560W_NORMAL_BM25_SEARCH_BENCHMARK_REGISTER(topK) \
    BENCHMARK_DEFINE_F(BM25, normal_##queryCount##_k_##topK)(benchmark::State& state) \
    { \
        PerformSearch(state, topK); \
    } \
    BENCHMARK_REGISTER_F(BM25, normal_##queryCount##_k_##topK) \
        ->Threads(1) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(BM25, normal_##queryCount##_k_##topK) \
        ->Threads(2) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(BM25, normal_##queryCount##_k_##topK) \
        ->Threads(4) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(BM25, normal_##queryCount##_k_##topK) \
        ->Threads(8) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond);


WIKI_560W_RANDOM_BM25_SEARCH_BENCHMARK_REGISTER(10, 1000);
WIKI_560W_RANDOM_BM25_SEARCH_BENCHMARK_REGISTER(100, 1000);
// WIKI_560W_RANDOM_BM25_SEARCH_BENCHMARK_REGISTER(200, 1000);
// WIKI_560W_RANDOM_BM25_SEARCH_BENCHMARK_REGISTER(500, 1000);

// WIKI_560W_NORMAL_BM25_SEARCH_BENCHMARK_REGISTER(10);
// WIKI_560W_NORMAL_BM25_SEARCH_BENCHMARK_REGISTER(100);
// WIKI_560W_NORMAL_BM25_SEARCH_BENCHMARK_REGISTER(200);
// WIKI_560W_NORMAL_BM25_SEARCH_BENCHMARK_REGISTER(500);