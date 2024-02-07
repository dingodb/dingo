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
#include <chrono>
#include <thread>
#include <mutex>

using json = nlohmann::json;

using namespace std;
namespace fs = std::filesystem;


class SkipIndex : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) override {
        InitializeResources();
        tantivy_load_index(this->indexDirectory);
    }

    void TearDown(const ::benchmark::State& state) override {
        tantivy_reader_free(this->indexDirectory);
    }

    void PerformSearch(benchmark::State& state, size_t granuleSize) {
        // Search for all given terms.
        SearchImpl(state, granuleSize, this->allIndices);
    }

    void PerformRandomSearch(benchmark::State& state, size_t queryCount, size_t granuleSize) {
        // Only random choose some terms to search.
        auto randomIndexes = WikiDatasetLoader::getInstance().generateRandomArray(queryCount, 0, this->queryTerms.size());
        SearchImpl(state, granuleSize, randomIndexes);
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

    void SearchImpl(benchmark::State& state, size_t granuleSize, const std::vector<size_t>& indexes) {
        auto rowIdRanges = WikiDatasetLoader::getInstance().getRowIdRanges(granuleSize);

        // `queries` records the total number of iterations conducted with `table` as the unit of traversal.
        // `granule_queries` records the total number of iterations conducted with `granule` as the unit of traversal.
        uint64_t queries = 0, granule_queries = 0;

        for (auto _ : state) {
            for (auto i : indexes) {
                for (const auto& range : rowIdRanges) {
                    tantivy_search_in_rowid_range(
                        this->indexDirectory,
                        this->queryTerms[i], 
                        range, 
                        range + granuleSize, 
                        false
                    );
                }
                granule_queries += rowIdRanges.size();
            }
            queries += indexes.size();
        }

        state.counters["QPS"] = benchmark::Counter(queries, benchmark::Counter::kIsRate);
        state.counters["QPS(avgThreads)"] = benchmark::Counter(queries, benchmark::Counter::kAvgThreadsRate);
    }
};

#define WIKI_560W_SKIPINDEX_RANDOM_SEARCH_BENCHMARK_REGISTER(queryCount, granuleSize) \
    BENCHMARK_DEFINE_F(SkipIndex, wiki5m_rand_##queryCount##_ig_##granuleSize)(benchmark::State& state) \
    { \
        PerformRandomSearch(state, queryCount, granuleSize); \
    } \
    BENCHMARK_REGISTER_F(SkipIndex, wiki5m_rand_##queryCount##_ig_##granuleSize) \
        ->Threads(1) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(SkipIndex, wiki5m_rand_##queryCount##_ig_##granuleSize) \
        ->Threads(2) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(SkipIndex, wiki5m_rand_##queryCount##_ig_##granuleSize) \
        ->Threads(4) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(SkipIndex, wiki5m_rand_##queryCount##_ig_##granuleSize) \
        ->Threads(8) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond);

#define WIKI_560W_SKIPINDEX_NORMAL_SEARCH_BENCHMARK_REGISTER(granuleSize) \
    BENCHMARK_DEFINE_F(SkipIndex, wiki5m_normal_ig_##granuleSize)(benchmark::State& state) \
    { \
        PerformSearch(state, granuleSize); \
    } \
    BENCHMARK_REGISTER_F(SkipIndex, wiki5m_normal_ig_##granuleSize) \
        ->Threads(1) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(SkipIndex, wiki5m_normal_ig_##granuleSize) \
        ->Threads(2) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(SkipIndex, wiki5m_normal_ig_##granuleSize) \
        ->Threads(4) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(SkipIndex, wiki5m_normal_ig_##granuleSize) \
        ->Threads(8) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond);


WIKI_560W_SKIPINDEX_RANDOM_SEARCH_BENCHMARK_REGISTER(1000, 8192)
WIKI_560W_SKIPINDEX_RANDOM_SEARCH_BENCHMARK_REGISTER(1000, 2048)

WIKI_560W_SKIPINDEX_NORMAL_SEARCH_BENCHMARK_REGISTER(8192)
