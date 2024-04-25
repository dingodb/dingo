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


class IndexOneColumn : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) override {
        ffi_create_index(QueryTerms::TANTIVY_INDEX_FILES_PATH, {"col1"});
    }

    void TearDown(const ::benchmark::State& state) override {
        ffi_free_index_reader(QueryTerms::TANTIVY_INDEX_FILES_PATH);
        ffi_free_index_writer(QueryTerms::TANTIVY_INDEX_FILES_PATH);
    }

    void IndexOneColumn1MDoc(benchmark::State& state) {
        uint64_t indexed = 0;
        for (auto _ : state) {
            for (size_t i = 0; i < 1000000; i++)
            {
                ffi_index_multi_column_docs(
                    QueryTerms::TANTIVY_INDEX_FILES_PATH,
                    i,
                    {"col1"},
                    {QueryTerms::DOCUMENTS[i].body}
                );
            }
            ffi_index_writer_commit(QueryTerms::TANTIVY_INDEX_FILES_PATH);
            indexed += 1000000;
        }
        state.counters["QPS"] = benchmark::Counter(indexed, benchmark::Counter::kIsRate);
        state.counters["QPS(avgThreads)"] = benchmark::Counter(indexed, benchmark::Counter::kAvgThreadsRate);
    }  
};

class IndexThreeColumn : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) override {
        ffi_create_index(QueryTerms::TANTIVY_INDEX_FILES_PATH, {"col1", "col2", "col3"});
    }

    void TearDown(const ::benchmark::State& state) override {
        ffi_free_index_reader(QueryTerms::TANTIVY_INDEX_FILES_PATH);
        ffi_free_index_writer(QueryTerms::TANTIVY_INDEX_FILES_PATH);
    }

    void IndexThreeColumn1MDoc(benchmark::State& state) {
        uint64_t indexed = 0;
        for (auto _ : state) {
            for (size_t i = 0; i < 1000000; i++)
            {
                ffi_index_multi_column_docs(
                    QueryTerms::TANTIVY_INDEX_FILES_PATH,
                    i,
                    {"col1", "col2", "col3"},
                    {QueryTerms::DOCUMENTS[i].body, QueryTerms::DOCUMENTS[i].body, QueryTerms::DOCUMENTS[i].body}
                );
            }
            ffi_index_writer_commit(QueryTerms::TANTIVY_INDEX_FILES_PATH);
            indexed += 1000000;
        }
        state.counters["QPS"] = benchmark::Counter(indexed, benchmark::Counter::kIsRate);
        state.counters["QPS(avgThreads)"] = benchmark::Counter(indexed, benchmark::Counter::kAvgThreadsRate);
    }  
};

#define WIKI_560W_Index_One_Column_BenchmarkRegister() \
    BENCHMARK_DEFINE_F(IndexOneColumn, wiki5m_index_1m_docs)(benchmark::State& state) \
    { \
        IndexOneColumn1MDoc(state); \
    } \
    BENCHMARK_REGISTER_F(IndexOneColumn, wiki5m_index_1m_docs) \
        ->Threads(1) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(IndexOneColumn, wiki5m_index_1m_docs) \
        ->Threads(8) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond);

#define WIKI_560W_Index_Three_Column_BenchmarkRegister() \
    BENCHMARK_DEFINE_F(IndexThreeColumn, wiki5m_index_1m_docs)(benchmark::State& state) \
    { \
        IndexThreeColumn1MDoc(state); \
    } \
    BENCHMARK_REGISTER_F(IndexThreeColumn, wiki5m_index_1m_docs) \
        ->Threads(1) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond); \
    BENCHMARK_REGISTER_F(IndexThreeColumn, wiki5m_index_1m_docs) \
        ->Threads(8) \
        ->Iterations(4) \
        ->Unit(benchmark::kMillisecond);


WIKI_560W_Index_One_Column_BenchmarkRegister();
WIKI_560W_Index_Three_Column_BenchmarkRegister();
