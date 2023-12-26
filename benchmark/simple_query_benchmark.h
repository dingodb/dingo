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

using json = nlohmann::json;

using namespace std;



struct Doc
{
    int id;
    std::string title;
    std::string body;
};

// from json to Doc.
void from_json(const json &j, Doc &doc)
{
    j.at("id").get_to(doc.id);
    j.at("title").get_to(doc.title);
    j.at("body").get_to(doc.body);
}

size_t index_docs_from_json(const std::string &json_file_path, const std::string &index_path)
{
    std::ifstream file(json_file_path);

    // parase JSON
    json j;
    file >> j;

    // from json file to Doc vector
    std::vector<Doc> docs = j.get<std::vector<Doc>>();

    TantivySearchIndexW *indexW = tantivy_create_index(index_path.c_str());

    // index all docs
    size_t row_id = 0;
    for (const auto &doc : docs)
    {
        tantivy_index_doc(indexW, row_id, doc.body.c_str());
        row_id += 1;
        // each doc call commit will slower the index build time.
        // tantivy_writer_commit(indexW);
    }
    tantivy_writer_commit(indexW);
    tantivy_writer_free(indexW);
    return row_id;
}

std::vector<uint64_t> generate_array(std::size_t step, std::size_t lrange, std::size_t rrange) {
    std::vector<uint64_t> array;
    std::size_t size = (rrange - lrange) / step + 1; 
    array.reserve(size);

    for (uint64_t i = lrange; i <= rrange; i += step) {
        array.push_back(i);
    }

    return array;
}


class SearchBenchmark : public benchmark::Fixture {
public:
    std::vector<size_t> row_id_range;
    size_t row_id_step=8192;
    size_t total_rows=5600000;
    std::vector<std::string> terms;
    std::string query_term_path = "/home/mochix/workspace_github/tantivy-search/examples/query_terms.json";
    std::string index_path = "/home/mochix/tantivy_search_memory/index_path";
    

    void SetUp(const ::benchmark::State& state) override {
        std::ifstream file(query_term_path);
        json j;
        file >> j;
        terms = j["terms"];
        file.close();
        row_id_range = generate_array(row_id_step, 0, total_rows);
    }

    void TearDown(const ::benchmark::State& state) override {
        terms.clear();
        row_id_range.clear();
    }
};

BENCHMARK_DEFINE_F(SearchBenchmark, SearchOperation)(benchmark::State& state){
    for (auto _ : state) {
        TantivySearchIndexR *indexR = tantivy_load_index(index_path.c_str());
        for (size_t i = 0; i < terms.size(); i++) {
            for (size_t j = 0; j < row_id_range.size(); j++) {
                tantivy_search_in_rowid_range(indexR, terms[i].c_str(), row_id_range[j], row_id_range[j] + row_id_step, false);
            }
        }
        tantivy_reader_free(indexR);
    }
}

BENCHMARK_REGISTER_F(SearchBenchmark, SearchOperation)->Threads(16)->Iterations(100);