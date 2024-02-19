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
        Search1KImpl(state, topK);
    }

private:
    mutex resourceMutex;
    vector<string> queryTerms;
    string indexDirectory;

    void InitializeResources(){
        lock_guard<mutex> lock(resourceMutex);
        this->queryTerms = WikiDatasetLoader::getInstance().loadQueryTerms();
        this->indexDirectory = WikiDatasetLoader::getInstance().getIndexDirectory(); 
    }

    void Search1KImpl(benchmark::State& state, size_t topK) {
        // `queries` records the total number of bm25 queries.
        uint64_t queries = 0;
        for (auto _ : state) {
            for (size_t i = 0; i < 1000; i++)
            {
                tantivy_bm25_search(this->indexDirectory, this->queryTerms[ (i+queries)%queryTerms.size() ], topK, false);
            }
            queries += 1000;
        }

        state.counters["QPS"] = benchmark::Counter(queries, benchmark::Counter::kIsRate);
        state.counters["QPS(avgThreads)"] = benchmark::Counter(queries, benchmark::Counter::kAvgThreadsRate);
    }
};

#define WIKI_560W_QUERY1K_BM25_SEARCH_BENCHMARK_REGISTER(topK) \
    BENCHMARK_DEFINE_F(BM25, query1k_top_##topK)(benchmark::State& state) \
    { \
        PerformSearch(state, topK); \
    } \
    BENCHMARK_REGISTER_F(BM25, query1k_top_##topK) \
        ->Threads(1) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(BM25, query1k_top_##topK) \
        ->Threads(2) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(BM25, query1k_top_##topK) \
        ->Threads(4) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(BM25, query1k_top_##topK) \
        ->Threads(8) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond);


WIKI_560W_QUERY1K_BM25_SEARCH_BENCHMARK_REGISTER(10);
WIKI_560W_QUERY1K_BM25_SEARCH_BENCHMARK_REGISTER(500);