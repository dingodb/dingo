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
using json = nlohmann::json;

using namespace std;
namespace fs = std::filesystem;


class Wiki560wSkipIndexSearchBenchmark : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) override {
        tantivy_load_index(WikiDatasetLoader::getInstance().getIndexDirectory());
    }

    void TearDown(const ::benchmark::State& state) override {
        tantivy_reader_free(WikiDatasetLoader::getInstance().getIndexDirectory());
    }


    void PerformSearch(benchmark::State& state, size_t granuleSize) {
        // Search for all given terms.
        auto query_terms = WikiDatasetLoader::getInstance().loadQueryTerms();
        std::vector<size_t> all_indexes(query_terms.size());
        std::iota(all_indexes.begin(), all_indexes.end(), 0); // generate indices from 0 to query_terms.size()
        SearchImpl(state, granuleSize, all_indexes);
    }

    void PerformRandomSearch(benchmark::State& state, size_t queryCount, size_t granuleSize) {
        // Only random choose some terms to search.
        auto query_terms = WikiDatasetLoader::getInstance().loadQueryTerms();
        auto random_indexes = WikiDatasetLoader::getInstance().generateRandomArray(queryCount, 0, query_terms.size());
        SearchImpl(state, granuleSize, random_indexes);
    }
private:
    void SearchImpl(benchmark::State& state, size_t granuleSize, const std::vector<size_t>& indexes) {
        auto index_directory = WikiDatasetLoader::getInstance().getIndexDirectory();
        auto row_id_ranges = WikiDatasetLoader::getInstance().getRowIdRanges(granuleSize);
        auto query_terms = WikiDatasetLoader::getInstance().loadQueryTerms();

        // `queries` records the total number of iterations conducted with `table` as the unit of traversal.
        // `granule_queries` records the total number of iterations conducted with `granule` as the unit of traversal.
        uint64_t queries = 0, granule_queries = 0;

        for (auto _ : state) {
            for (auto i : indexes) {
                for (const auto& range : row_id_ranges) {
                    tantivy_search_in_rowid_range(
                        index_directory,
                        query_terms[i], 
                        range, 
                        range + granuleSize, 
                        false
                    );
                }
                granule_queries += row_id_ranges.size();
            }
            queries += indexes.size();
        }

        // Record custom counters
        state.counters["Queries"] = queries;
        // state.counters["FFI_Queries"] = granule_queries;
        state.counters["QPS(total)"] = benchmark::Counter(queries, benchmark::Counter::kIsRate);
        // state.counters["QPS_FFI(total)"] = benchmark::Counter(granule_queries, benchmark::Counter::kIsRate);
        state.counters["QPS(avgThreads)"] = benchmark::Counter(queries, benchmark::Counter::kAvgThreadsRate);
        // state.counters["QPS_FFI(avgThreads)"] = benchmark::Counter(granule_queries, benchmark::Counter::kAvgThreadsRate);
    }
};

#define WIKI_560W_SKIPINDEX_RANDOM_SEARCH_BENCHMARK_REGISTER(queryCount, granuleSize) \
    BENCHMARK_DEFINE_F(Wiki560wSkipIndexSearchBenchmark, SkipGranule_RandomQuery_##queryCount##_Terms_Wiki560w_Granule_##granuleSize)(benchmark::State& state) \
    { \
        PerformRandomSearch(state, queryCount, granuleSize); \
    } \
    BENCHMARK_REGISTER_F(Wiki560wSkipIndexSearchBenchmark, SkipGranule_RandomQuery_##queryCount##_Terms_Wiki560w_Granule_##granuleSize) \
        ->Threads(1) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(Wiki560wSkipIndexSearchBenchmark, SkipGranule_RandomQuery_##queryCount##_Terms_Wiki560w_Granule_##granuleSize) \
        ->Threads(2) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(Wiki560wSkipIndexSearchBenchmark, SkipGranule_RandomQuery_##queryCount##_Terms_Wiki560w_Granule_##granuleSize) \
        ->Threads(4) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(Wiki560wSkipIndexSearchBenchmark, SkipGranule_RandomQuery_##queryCount##_Terms_Wiki560w_Granule_##granuleSize) \
        ->Threads(8) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond);

#define WIKI_560W_SKIPINDEX_NORMAL_SEARCH_BENCHMARK_REGISTER(granuleSize) \
    BENCHMARK_DEFINE_F(Wiki560wSkipIndexSearchBenchmark, SkipGranule_NormalQuery_Wiki560w_Granule_##granuleSize)(benchmark::State& state) \
    { \
        PerformSearch(state, granuleSize); \
    } \
    BENCHMARK_REGISTER_F(Wiki560wSkipIndexSearchBenchmark, SkipGranule_NormalQuery_Wiki560w_Granule_##granuleSize) \
        ->Threads(1) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(Wiki560wSkipIndexSearchBenchmark, SkipGranule_NormalQuery_Wiki560w_Granule_##granuleSize) \
        ->Threads(2) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(Wiki560wSkipIndexSearchBenchmark, SkipGranule_NormalQuery_Wiki560w_Granule_##granuleSize) \
        ->Threads(4) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(Wiki560wSkipIndexSearchBenchmark, SkipGranule_NormalQuery_Wiki560w_Granule_##granuleSize) \
        ->Threads(8) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond);


WIKI_560W_SKIPINDEX_RANDOM_SEARCH_BENCHMARK_REGISTER(1000, 8192)
// WIKI_560W_SKIPINDEX_RANDOM_SEARCH_BENCHMARK_REGISTER(1000, 2048)
// WIKI_560W_SKIPINDEX_RANDOM_SEARCH_BENCHMARK_REGISTER(1000, 1024)
WIKI_560W_SKIPINDEX_RANDOM_SEARCH_BENCHMARK_REGISTER(1000, 128)

// WIKI_560W_SKIPINDEX_NORMAL_SEARCH_BENCHMARK_REGISTER(8192)
// WIKI_560W_SKIPINDEX_NORMAL_SEARCH_BENCHMARK_REGISTER(2048)
// WIKI_560W_SKIPINDEX_NORMAL_SEARCH_BENCHMARK_REGISTER(1024)
// WIKI_560W_SKIPINDEX_NORMAL_SEARCH_BENCHMARK_REGISTER(128)
