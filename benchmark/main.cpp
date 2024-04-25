#include <SkipIndexBenchmark.h>
#include <BM25SearchBenchmark.h>
#include <IndexDocumentBenchmark.h>
#include <benchmark/benchmark.h>
#include <BenchmarkConfig.h>
using namespace std;
namespace  bpo = boost::program_options;

// Run the benchmark
int main(int argc, char** argv) {
    string index_path;
    string query_term_path;
    string docs_path;
    bool skip_build_index;

    bpo::options_description desc("Benchmark Options");
    desc.add_options()
    ("index-path,ip", bpo::value<std::string>(&index_path)->default_value("/tmp/tantivy_search/benchmark/index_path"), "tantivy index files directory")
    ("query-term-path,qtp", bpo::value<std::string>(&query_term_path)->default_value("query_terms.json"), "query terms json file path")
    ("docs-path,dp", bpo::value<std::string>(&docs_path)->default_value("wiki_560w.json"), "docs json file path")
    ("skip-build-index,sbi", bpo::value<bool>(&skip_build_index)->default_value(false), "if need skip build index")
    ("help", "this is help message");


   try {
        bpo::variables_map vm;
        bpo::store(bpo::parse_command_line(argc, argv, desc), vm);
        bpo::notify(vm);
        if(vm.count("help")) {
            return 0;
        }


        char arg0_default[] = "benchmark";
        char* args_default = arg0_default;
        if (!argv) {
        argc = 1;
        argv = &args_default;
        }

        // Prepare for benchmark.
        if (!skip_build_index){
            std::cout << "Building index for WikiSkipIndexSearchBenchmark" << endl;
            index_docs_from_json(docs_path, index_path);
            std::cout << "Build finished." << endl;
        }
        WikiDatasetLoader::getInstance().loadQueryTerms(query_term_path);
        WikiDatasetLoader::getInstance().setIndexDirectory(index_path);
        WikiDatasetLoader::getInstance().setDatasetFilePath(docs_path);
        QueryTerms::initializeQueryTerms();
        tantivy_search_log4rs_initialize("./log", "info", true, false, false);


        // Run all benchmark
        int benchmark_argc = 2;
        char* benchmark_program = argv[0];
        char benchmark_tabular_arg[] = "--benchmark_counters_tabular=true";
        char* benchmark_argv[] = { benchmark_program, benchmark_tabular_arg };
        ::benchmark::Initialize(&benchmark_argc, benchmark_argv);
        if (::benchmark::ReportUnrecognizedArguments(benchmark_argc, benchmark_argv)) return 1;
        ::benchmark::RunSpecifiedBenchmarks();
        ::benchmark::Shutdown();
        return 0;
    } catch (const bpo::error &e) {
        return 1;
    }
}