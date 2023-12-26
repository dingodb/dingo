#include "simple_query_benchmark.h"
#include <benchmark/benchmark.h>
using namespace std;
namespace  bpo = boost::program_options;

// Run the benchmark
int main(int argc, char** argv) {
    string index_path;
    string query_term_path;

    bpo::options_description desc("Benchmark Options");
    desc.add_options()
    ("index-path,ip", bpo::value<std::string>(&index_path)->default_value("/home/mochix/tantivy_search_memory/index_path"), "tantivy index files path")
    ("query-term-path,qtp", bpo::value<std::string>(&query_term_path)->default_value("/home/mochix/workspace_github/tantivy-search/examples/query_terms.json"), "query terms file path")
    ("help", "this is help message");

   try {
        bpo::variables_map vm;
        bpo::store(bpo::parse_command_line(argc, argv, desc), vm);
        bpo::notify(vm);
        if(vm.count("help")) {
            return 0;
        }
    } catch (const bpo::error &e) {
        return 1;
    }

    char arg0_default[] = "benchmark";
    char* args_default = arg0_default;
    if (!argv) {
      argc = 1;
      argv = &args_default;
    }

    // SearchBenchmark::query_term_path = "/home/mochix/workspace_github/tantivy-search/examples/query_terms.json";

    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}