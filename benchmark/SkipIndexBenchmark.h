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


class WikiSkipIndexSearchBenchmark : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) override {
        tantivy_load_index(WikiDatasetLoader::getInstance().getIndexDirectory());
    }

    void TearDown(const ::benchmark::State& state) override {
        tantivy_reader_free(WikiDatasetLoader::getInstance().getIndexDirectory());
    }
};

BENCHMARK_DEFINE_F(WikiSkipIndexSearchBenchmark, SkipGranule1K)(benchmark::State& state){
    auto index_directory = WikiDatasetLoader::getInstance().getIndexDirectory();
    auto row_id_ranges = WikiDatasetLoader::getInstance().getRowIdRanges();
    auto granule_step = WikiDatasetLoader::getInstance().getGranuleStep();
    auto query_terms = WikiDatasetLoader::getInstance().loadQueryTerms();
    for (auto _ : state) {
        for (size_t i = 0; i < 1000; i++) {
            for (size_t j = 0; j < row_id_ranges.size(); j++) {
                tantivy_search_in_rowid_range(
                    index_directory,
                    query_terms[i], 
                    row_id_ranges[j], 
                    row_id_ranges[j] + granule_step, 
                    false);
            }
        }
    }
}


BENCHMARK_REGISTER_F(WikiSkipIndexSearchBenchmark, SkipGranule1K)
    ->Threads(16)
    ->Iterations(4)
    ->Unit(benchmark::kMillisecond);

