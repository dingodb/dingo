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
        InitializeResourcesOnce();
        ffi_load_index_reader(this->indexDirectory);
    }

    void TearDown(const ::benchmark::State& state) override {
        ffi_free_index_reader(this->indexDirectory);
    }

    void PerformSearch(benchmark::State& state, size_t granuleSize) {
        Search1KImpl(state, granuleSize);
    }

private:
    // mutex resourceMutex;
    std::once_flag flag;
    vector<string> queryTerms;
    string indexDirectory;

    void InitializeResources(){
        // lock_guard<mutex> lock(resourceMutex);
        this->queryTerms = WikiDatasetLoader::getInstance().loadQueryTerms();
        this->indexDirectory = WikiDatasetLoader::getInstance().getIndexDirectory(); 
    }
    void InitializeResourcesOnce(){
        call_once(flag, [this](){this->InitializeResources();});
    }

    void Search1KImpl(benchmark::State& state, size_t granuleSize) {
        // auto rowIdRanges = WikiDatasetLoader::getInstance().getRowIdRanges(granuleSize);
        // `queries` records the total number of iterations conducted with `table` as the unit of traversal.
        uint64_t queries = 0;

        for (auto _ : state) {
            for (size_t i = 0; i < 1000; i++)
            {
                // for (const auto& range : rowIdRanges) {
                ffi_query_term_bitmap(
                    this->indexDirectory,
                    "text",
                    this->queryTerms[ (i+queries)%queryTerms.size() ]
                    // range, 
                    // range + granuleSize
                    );
                // }
            }
            queries += 1000;
        }
        // std::cout<< "iteration count"<<state.iterations()<<" item processed"<<state.items_processed()<<std::endl;
        state.counters["QPS"] = benchmark::Counter(queries, benchmark::Counter::kIsRate);
        state.counters["QPS(avgThreads)"] = benchmark::Counter(queries, benchmark::Counter::kAvgThreadsRate);
    }
};


#define WIKI_560W_SKIPINDEX_QUERY1K_SEARCH_BENCHMARK_REGISTER(granuleSize) \
    BENCHMARK_DEFINE_F(SkipIndex, wiki5m_query1k_ig_##granuleSize)(benchmark::State& state) \
    { \
        PerformSearch(state, granuleSize); \
    } \
    BENCHMARK_REGISTER_F(SkipIndex, wiki5m_query1k_ig_##granuleSize) \
        ->Threads(1) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(SkipIndex, wiki5m_query1k_ig_##granuleSize) \
        ->Threads(2) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(SkipIndex, wiki5m_query1k_ig_##granuleSize) \
        ->Threads(4) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(SkipIndex, wiki5m_query1k_ig_##granuleSize) \
        ->Threads(8) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond);


WIKI_560W_SKIPINDEX_QUERY1K_SEARCH_BENCHMARK_REGISTER(8192)
WIKI_560W_SKIPINDEX_QUERY1K_SEARCH_BENCHMARK_REGISTER(128)
