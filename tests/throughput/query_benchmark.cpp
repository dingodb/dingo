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
#include <utils.h>
#include <unistd.h>
#include <boost/program_options.hpp>

using json = nlohmann::json;
using namespace std;
using namespace Utils;
namespace  bpo = boost::program_options;

// record bench_task latency 
std::vector<double> ms_latencies;
std::mutex ms_latencies_mutex;

// mark whether bench_task is running
bool is_benchmarking;
std::mutex is_benchmarking_mutex;

void setBenchmarking(bool value) {
        std::lock_guard<std::mutex> lock(is_benchmarking_mutex);
        is_benchmarking = value;
    }

bool getBenchmarking() {
    std::lock_guard<std::mutex> lock(is_benchmarking_mutex);
    return is_benchmarking;
}
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

size_t index_docs_from_json(const std::string &json_file_path, const std::string &index_path, const bool use_delete)
{
    std::ifstream file(json_file_path);

    // parase JSON
    json j;
    file >> j;

    // from json file to Doc vector
    std::vector<Doc> docs = j.get<std::vector<Doc>>();

    LOG(INFO) << "Cooling 10s, and then create/build index.";
    sleep(10);

    tantivy_create_index(index_path, false);

    // index all docs
    size_t row_id = 0;
    for (const auto &doc : docs)
    {
        tantivy_index_doc(index_path, row_id, doc.body.c_str());
        row_id += 1;
    }
    tantivy_writer_commit(index_path);
    if(!use_delete){
        tantivy_writer_free(index_path);
    }
    return row_id;
}


void record_millis_latency(double latency) {
    std::lock_guard<std::mutex> lock(ms_latencies_mutex);
    ms_latencies.push_back(latency);
}

void clear_millis_latency() {
    std::lock_guard<std::mutex> lock(ms_latencies_mutex);
    ms_latencies.clear();
}

double calculate_ms_percentile(int percentile) {
    std::lock_guard<std::mutex> lock(ms_latencies_mutex);
    if (ms_latencies.empty()) {
        return 0.0;
    }

    std::sort(ms_latencies.begin(), ms_latencies.end());
    int index = percentile * ms_latencies.size() / 100;
    return std::round(ms_latencies[index] * 1000.0) / 1000.0; // keep 3 decimal places
}

double calculate_ms_average() {
    std::lock_guard<std::mutex> lock(ms_latencies_mutex);
    if (ms_latencies.empty()) {
        return 0.0;
    }
    double sum = std::accumulate(ms_latencies.begin(), ms_latencies.end(), 0.0);
    double average = sum / ms_latencies.size();
    return std::round(average * 1000.0) / 1000.0; // keep 3 decimal places
}


// benchmark
void run_benchmark(size_t idx,                               // iter id
                   const std::vector<std::string>& terms,    // query terms
                   size_t each_benchmark_duration,           // benchmark time should smaller than each_benchmark_duration
                   ThreadPool& pool,                         // thread pool
                   std::string& index_path,                  // index reader
                   std::atomic<int>& query_count,            // how many terms have been queried
                   const std::vector<size_t>& row_id_range,  // row_id_range for queried
                   size_t row_id_step,                       // row_id_step is similar wih granule range 8192
                   std::atomic<size_t> &finished_tasks,      // how many tasks have been finished
                   std::atomic<size_t> &enqueue_tasks_count, // record enqueue task size.
                   std::size_t max_pending_tasks,            // `bench_task` max count in queue
                   bool use_bm25_search,                     // test `bm25_search`, rather than `skip-index`
                   bool use_topk_delete,                     // whether use random delete when test `bm25_search`. 
                   bool verify_delete_correct,               // when use topk_delete, verify it's results.
                   std::size_t bm25_search_topk,             // bm25_search topk
                   bool only_random_delete)                  // only use random k delete
{
    // mark benchmark should stop.
    std::atomic<bool> flag_break = false;
    // record benchmark start time.
    auto benchmark_start = std::chrono::high_resolution_clock::now();

    while (true) {
        if(flag_break){
            break;
        }
        for (size_t i = 0; i < terms.size(); i++) {
            // edge1. handle benchmark task time out.
            auto current = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::seconds>(current - benchmark_start).count();
            if(duration >= each_benchmark_duration){
                flag_break = true;
                break;
            }

            // edge2. pending queue size should not bigger than `max_pending_tasks`
            while ((enqueue_tasks_count-finished_tasks) >= max_pending_tasks)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }


            std::string term = std::string(terms[i]);
            pool.enqueue([idx, term, &row_id_range, row_id_step, index_path, &query_count, &finished_tasks, use_bm25_search, use_topk_delete, verify_delete_correct, bm25_search_topk, only_random_delete](){
                auto start_query = std::chrono::high_resolution_clock::now();
                if(use_bm25_search){
                    // for bm25_search test
                    if(only_random_delete){
                        // just delete random row ids.
                        std::vector<u_int32_t> delete_row_ids = randomExtractK<size_t, u_int32_t>(row_id_range, bm25_search_topk);
                        tantivy_delete_row_ids(index_path, delete_row_ids);
                    }else{
                        // Step1. first bm25 search.
                        rust::cxxbridge1::Vec<RowIdWithScore> result = tantivy_bm25_search(index_path, term, bm25_search_topk, false);
                        std::vector<uint32_t> row_ids;
                        for (size_t i = 0; i < result.size(); i++)
                            row_ids.push_back(static_cast<uint32_t>(result[i].row_id));
                        if(use_topk_delete){
                            // Step2. delete row_ids in first step.
                            tantivy_delete_row_ids(index_path, row_ids);
                            if(verify_delete_correct){
                                // Step3. research for verify result.
                                rust::cxxbridge1::Vec<RowIdWithScore> result_after_delete = tantivy_bm25_search(index_path, term, bm25_search_topk, false);
                                std::vector<uint32_t> row_ids_after_delete;
                                for (size_t i = 0; i < result_after_delete.size(); i++)
                                    row_ids_after_delete.push_back(static_cast<uint32_t>(result_after_delete[i].row_id));
                                size_t intersection = intersection_size(row_ids, row_ids_after_delete);
                                if(intersection!=0){
                                    LOG(ERROR) << "Error happend with delete operation, intersection size:"<< intersection;
                                }
                            }

                        }
                    }
                } else {
                    // for skip-index test.
                    for (size_t j = 0; j < row_id_range.size(); j++) {
                        tantivy_search_in_rowid_range(index_path, term, row_id_range[j], row_id_range[j] + row_id_step, false);
                    }
                }
                query_count++;
                auto end_query = std::chrono::high_resolution_clock::now();
                double latency = std::chrono::duration_cast<std::chrono::nanoseconds>(end_query - start_query).count() / 1000000.0;
                record_millis_latency(latency);
                // mark this bench_task finished.
                finished_tasks+=1;
            });
            // bench_task has been enqueued.
            enqueue_tasks_count += 1;
        }
    }
}

int main(int argc, char* argv[])
{
    initialize_easy_logger(el::Level::Info);
    log_level = 2;

    LOG(INFO) << "After 10s, program will start";
    LOG(INFO) << "##### PID:" << std::to_string(getpid());

    sleep(10);

    size_t pool_size;
    int each_benchmark_duration;
    bool skip_build_index;
    bool skip_benchmark;
    int each_free_wait;
    size_t iter_times;
    std::string index_path;
    std::string query_term_path;
    std::string dataset_path;
    size_t max_pending_tasks;
    bool use_bm25_search;
    bool use_topk_delete;
    bool verify_delete_correct;
    size_t bm25_search_topk;
    bool only_random_delete;


    bpo::options_description desc("Benchmark Options");
    desc.add_options()
    ("pool-size,ps", bpo::value<std::size_t>(&pool_size)->default_value(1), "pool size")
    ("each-benchmark-duration,ebd", bpo::value<int>(&each_benchmark_duration)->default_value(1200), "set benchmark duration")
    ("skip-build-index,sbi", bpo::value<bool>(&skip_build_index)->default_value(true), "skip build index")
    ("skip-benchmark,sb", bpo::value<bool>(&skip_benchmark)->default_value(false), "skip benchmark")
    ("each-free-wait,efw", bpo::value<int>(&each_free_wait)->default_value(120), "set free index reader duration")
    ("iter-times,it", bpo::value<std::size_t>(&iter_times)->default_value(30), "iter times")
    ("index-path,ip", bpo::value<std::string>(&index_path)->default_value("index_files_path"), "tantivy index files path")
    ("query-term-path,qtp", bpo::value<std::string>(&query_term_path)->default_value("query_terms.json"), "query terms file path")
    ("dataset-path,dp", bpo::value<std::string>(&dataset_path)->default_value("wiki_560w.json"), "dataset file path")
    ("max-pending-tasks,mpt", bpo::value<std::size_t>(&max_pending_tasks)->default_value(1000), "max pending tasks")
    ("use-bm25-search,ubs", bpo::value<bool>(&use_bm25_search)->default_value(false), "test bm25 search, not skip-index")
    ("use-topk-delete,utd", bpo::value<bool>(&use_topk_delete)->default_value(false), "use topk delete")
    ("verify-delete-correct,vdc", bpo::value<bool>(&verify_delete_correct)->default_value(true), "verify whether delete is correct")
    ("bm25-search-topk,bst", bpo::value<size_t>(&bm25_search_topk)->default_value(10), "select TopK docs by tantivy_bm25_search interface")
    ("only-random-delete,ord", bpo::value<bool>(&only_random_delete)->default_value(false), "only delete operation in a single bench_task")
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

    LOG(INFO) << "##### Pool Size: " << std::to_string(pool_size);
    LOG(INFO) << "##### Each Benchmark Duration: " << std::to_string(each_benchmark_duration);
    LOG(INFO) << "##### Skip Build Index: " << std::to_string(skip_build_index);
    LOG(INFO) << "##### Skip Benchmark: " << std::to_string(skip_benchmark);
    LOG(INFO) << "##### Each Free Wait: " << std::to_string(each_free_wait);
    LOG(INFO) << "##### Iter Times: " << std::to_string(iter_times);
    LOG(INFO) << "##### Index Path: " << index_path;
    LOG(INFO) << "##### Query Term Path: " << query_term_path;
    LOG(INFO) << "##### Dataset Path: " << dataset_path;
    LOG(INFO) << "##### Max Pending Tasks: " << std::to_string(max_pending_tasks);
    LOG(INFO) << "##### Use BM25 Search: " << use_bm25_search;
    LOG(INFO) << "##### Use TopK Delete: " << use_topk_delete;
    LOG(INFO) << "##### Verify Delete Correct: " << verify_delete_correct;
    LOG(INFO) << "##### BM25 Search TopK: " << bm25_search_topk;
    LOG(INFO) << "##### Only Random Delete: " << only_random_delete;

    size_t total_rows = 5600000;
    size_t row_id_step = 8192;
    tantivy_search_log4rs_with_callback("./tantivy_search_log/", "info", false, false, tantivy_easylogging_callback);

    ThreadPool pool(pool_size);
    std::atomic<int> query_count(0);
    std::atomic<bool> is_finished(false);

    LOG(INFO) << "Loading query terms to vector.";
    std::ifstream file(query_term_path);
    json j;
    file >> j;
    std::vector<std::string> terms = j["terms"];
    LOG(INFO) << "terms size is:" << std::to_string(terms.size());


    if (!skip_build_index)
    {
        LOG(INFO) << "Starting index docs from dataset:" << dataset_path;
        total_rows = index_docs_from_json(dataset_path, index_path, use_topk_delete||only_random_delete);
        LOG(INFO) << std::to_string(total_rows) << " docs has been indexed.";

    }
    auto row_id_range = generate_array(row_id_step, 0, total_rows);


    auto reporter_start = std::chrono::high_resolution_clock::now();
    std::thread reporter([&]() {
        while (!is_finished) {
            std::this_thread::sleep_for(std::chrono::seconds(10));
            if (!getBenchmarking()){
                continue;
            }

            // calculate average qps
            auto now = std::chrono::high_resolution_clock::now();
            auto current_benchmark_duration = std::chrono::duration_cast<std::chrono::seconds>(now - reporter_start).count();
            int total_queries = query_count.load();
            int qps = current_benchmark_duration > 0 ? total_queries / static_cast<double>(current_benchmark_duration) : 0;
            // calculate p/xx
            double p10 = calculate_ms_percentile(10);
            double p30 = calculate_ms_percentile(30);
            double p50 = calculate_ms_percentile(50);
            double p70 = calculate_ms_percentile(70);
            double p99 = calculate_ms_percentile(99);
            double average_latency = calculate_ms_average();
            std::ostringstream oss;
            oss << "queue: " << pool.getTaskQueueSize() 
                << ", query: " << query_count << "(" << query_count/terms.size() << ")"
                << ", duration: " << current_benchmark_duration 
                << "s | qps: " << qps
                << std::fixed << std::setprecision(3) 
                << ", avg_lt: " << average_latency 
                << " ms, p10: " << p10 << " ms, p30: " << p30 << " ms, p50: " << p50 
                << " ms, p70: " << p70 << " ms, p99: " << p99 << " ms";
            LOG(INFO) << oss.str();
        } });


    LOG(INFO) << "Starting benchmark, pool:"<< pool_size << " iter times:"<< iter_times;
    for (size_t i = 0; i < iter_times; i++)
    {
        std::atomic<size_t> finished_tasks = 0; // already finished task count
        std::atomic<size_t> enqueue_tasks_count = 0; // task which just enqueued
        LOG(INFO) << "[iter-"<<i<<"] Loading tantivy index from index_path after " << each_free_wait << " sec.";
        sleep(each_free_wait);
        setBenchmarking(true);
        reporter_start = std::chrono::high_resolution_clock::now();
        tantivy_load_index(index_path);
        if (!skip_benchmark){
            run_benchmark(
                i, 
                terms, 
                each_benchmark_duration, 
                pool, 
                index_path, 
                query_count, 
                row_id_range, 
                row_id_step, 
                finished_tasks, 
                enqueue_tasks_count, 
                max_pending_tasks,
                use_bm25_search,
                use_topk_delete,
                verify_delete_correct,
                bm25_search_topk,
                only_random_delete
            );
        }
        while (pool.getTaskQueueSize()>0 || finished_tasks < enqueue_tasks_count)
        {
            LOG(INFO) << "[iter-"<<i<<"] Waiting benchmark pool["<<pool.getTaskQueueSize()<<"] finish ["<<finished_tasks<<"/"<<enqueue_tasks_count<<"]";
            sleep(2);
        }
        LOG(INFO) << "[iter-"<<i<<"] Trying free index reader; benchmark pool["<<pool.getTaskQueueSize()<<"] # ["<<finished_tasks<<"/"<<enqueue_tasks_count<<"]";
        tantivy_reader_free(index_path);
        clear_millis_latency();
        setBenchmarking(false);
    }
    

    is_finished = true;
    reporter.join();

    LOG(INFO) << "Waiting 10s stop this program";
    sleep(10);

    return 0;
}
