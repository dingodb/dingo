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


class WikiBm25SearchBenchmark : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) override {
        tantivy_load_index(WikiDatasetLoader::getInstance().getIndexDirectory());
    }

    void TearDown(const ::benchmark::State& state) override {
        tantivy_reader_free(WikiDatasetLoader::getInstance().getIndexDirectory());
    }
};

BENCHMARK_DEFINE_F(WikiBm25SearchBenchmark, Top10Bm25Search1K)(benchmark::State& state){
    auto index_directory = WikiDatasetLoader::getInstance().getIndexDirectory();
    auto row_id_ranges = WikiDatasetLoader::getInstance().getRowIdRanges();
    auto granule_step = WikiDatasetLoader::getInstance().getGranuleStep();
    auto query_terms = WikiDatasetLoader::getInstance().loadQueryTerms();

    size_t batch_search = 1000;

    for (auto _ : state) {
        for (size_t i = 0; i < batch_search; i++) {
            tantivy_bm25_search(index_directory, query_terms[i%batch_search], 10, false);
        }
    }
}

BENCHMARK_DEFINE_F(WikiBm25SearchBenchmark, Top100Bm25Search1K)(benchmark::State& state){
    auto index_directory = WikiDatasetLoader::getInstance().getIndexDirectory();
    auto row_id_ranges = WikiDatasetLoader::getInstance().getRowIdRanges();
    auto granule_step = WikiDatasetLoader::getInstance().getGranuleStep();
    auto query_terms = WikiDatasetLoader::getInstance().loadQueryTerms();

    size_t batch_search = 1000;

    for (auto _ : state) {
        for (size_t i = 0; i < batch_search; i++) {
            tantivy_bm25_search(index_directory, query_terms[i%batch_search], 100, false);
        }
    }
}


BENCHMARK_REGISTER_F(WikiBm25SearchBenchmark, Top10Bm25Search1K)
    ->Threads(16)
    ->Iterations(4)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(WikiBm25SearchBenchmark, Top100Bm25Search1K)
    ->Threads(16)
    ->Iterations(4)
    ->Unit(benchmark::kMillisecond);
