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
// #include <easylogging++.h>

// #include <jemalloc/jemalloc.h>
using json = nlohmann::json;

using namespace std;
using namespace Utils;
namespace  bpo = boost::program_options;

// 记录 benchmark 时延
std::vector<double> ms_latencies;
std::mutex ms_latencies_mutex;

// 标记是否正在进行 benchmark
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
    return std::round(ms_latencies[index] * 1000.0) / 1000.0; // 四舍五入到三位小数
}

double calculate_ms_average() {
    std::lock_guard<std::mutex> lock(ms_latencies_mutex);
    if (ms_latencies.empty()) {
        return 0.0;
    }
    double sum = std::accumulate(ms_latencies.begin(), ms_latencies.end(), 0.0);
    double average = sum / ms_latencies.size();
    return std::round(average * 1000.0) / 1000.0; // 四舍五入到三位小数
}


// benchmark
// 未超时则继续放入任务
void run_benchmark(size_t idx,                              // iter id
                   const std::vector<std::string>& terms,   // query terms
                   size_t each_benchmark_duration,          // benchmark time should smaller than each_benchmark_duration
                   ThreadPool& pool,                        // thread pool
                   TantivySearchIndexR* indexR,             // index reader
                   std::atomic<int>& query_count,           // how many terms have been queried
                   const std::vector<size_t>& row_id_range, // row_id_range for queried
                   size_t row_id_step,                      // row_id_step is similar wih granule range 8192
                   std::atomic<size_t> &finished_tasks,     // how many tasks have been finished
                   std::atomic<size_t> &enqueue_tasks_count, // record enqueue task size.
                   std::size_t max_pending_tasks )          // 
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
            // edge1. 判断 benchmark 是否超时, 超时了就需要及时停止
            auto current = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::seconds>(current - benchmark_start).count();
            if(duration >= each_benchmark_duration){
                flag_break = true;
                break;
            }

            // edge2. 约束 pending queue 长度不超过 max_pending_tasks
            while ((enqueue_tasks_count-finished_tasks) >= max_pending_tasks)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }


            std::string term = std::string(terms[i]);
            pool.enqueue([idx, term, &row_id_range, row_id_step, indexR, &query_count, &finished_tasks](){
                auto start_query = std::chrono::high_resolution_clock::now();
                for (size_t j = 0; j < row_id_range.size(); j++) {
                    tantivy_search_in_rowid_range(indexR, term.c_str(), row_id_range[j], row_id_range[j] + row_id_step, false);
                }
                query_count++;
                auto end_query = std::chrono::high_resolution_clock::now();
                double latency = std::chrono::duration_cast<std::chrono::nanoseconds>(end_query - start_query).count() / 1000000.0;
                record_millis_latency(latency);
                // 标记 task 已经完成
                finished_tasks+=1;
            });
            // task 以及入队
            enqueue_tasks_count += 1;
        }
    }
}

int main(int argc, char* argv[])
{
    initialize_easy_logger(el::Level::Info);
    log_level = 1;

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

    bpo::options_description desc("Benchmark Options");
    desc.add_options()
    ("pool-size,ps", bpo::value<std::size_t>(&pool_size)->default_value(1), "pool size")
    ("each-benchmark-duration,ebd", bpo::value<int>(&each_benchmark_duration)->default_value(1200), "set benchmark duration")
    ("skip-build-index,sbi", bpo::value<bool>(&skip_build_index)->default_value(true), "skip build index")
    ("skip-benchmark,sb", bpo::value<bool>(&skip_benchmark)->default_value(false), "skip benchmark")
    ("each-free-wait,efw", bpo::value<int>(&each_free_wait)->default_value(120), "set free index reader duration")
    ("iter-times,it", bpo::value<std::size_t>(&iter_times)->default_value(30), "iter times")
    ("index-path,ip", bpo::value<std::string>(&index_path)->default_value("/home/mochix/tantivy_search_memory/index_path"), "tantivy index files path")
    ("query-term-path,qtp", bpo::value<std::string>(&query_term_path)->default_value("/home/mochix/workspace_github/tantivy-search/examples/query_terms.json"), "query terms file path")
    ("dataset-path,dp", bpo::value<std::string>(&dataset_path)->default_value("/home/mochix/workspace_github/tantivy-search/examples/wiki_560w.json"), "dataset file path")
    ("max-pending-tasks,mpt", bpo::value<std::size_t>(&max_pending_tasks)->default_value(1000), "max pending tasks")
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

    size_t total_rows = 5600000;
    size_t row_id_step = 8192;
    tantivy_logger_initialize("./log", "info", false, tantivy_log_callback, false, false);

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
        total_rows = index_docs_from_json(dataset_path, index_path);
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
            double average_latency = calculate_ms_percentile(99);
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
        std::atomic<size_t> finished_tasks = 0; // 已经完成的 task 数量
        std::atomic<size_t> enqueue_tasks_count = 0; // 已经入队的 task 数量
        LOG(INFO) << "[iter-"<<i<<"] Loading tantivy index from index_path after " << each_free_wait << " sec.";
        sleep(each_free_wait);
        setBenchmarking(true);
        reporter_start = std::chrono::high_resolution_clock::now();
        TantivySearchIndexR *indexR = tantivy_load_index(index_path.c_str());
        if (!skip_benchmark){
            run_benchmark(i, terms, each_benchmark_duration, pool, indexR, query_count, row_id_range, row_id_step, finished_tasks, enqueue_tasks_count, max_pending_tasks);
        }
        while (pool.getTaskQueueSize()>0 || finished_tasks < enqueue_tasks_count)
        {
            LOG(INFO) << "[iter-"<<i<<"] Waiting benchmark pool["<<pool.getTaskQueueSize()<<"] finish ["<<finished_tasks<<"/"<<enqueue_tasks_count<<"]";
            sleep(2);
        }
        LOG(INFO) << "[iter-"<<i<<"] Trying free index reader; benchmark pool["<<pool.getTaskQueueSize()<<"] # ["<<finished_tasks<<"/"<<enqueue_tasks_count<<"]";
        tantivy_reader_free(indexR);
        clear_millis_latency();
        setBenchmarking(false);
    }
    

    is_finished = true;
    reporter.join();

    mylog(1, "Waiting 10s stop this program");
    sleep(10);

    return 0;
}
