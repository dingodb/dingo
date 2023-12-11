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
using json = nlohmann::json;

using namespace std;

static std::string log_level = "info";
std::vector<long long> nano_latencies; // 用于存储每个任务的完成时间 ns
std::mutex nano_latencies_mutex; // 保护 nano_latencies 的互斥量

std::vector<uint64_t> generate_array(std::size_t step, std::size_t lrange, std::size_t rrange) {
    std::vector<uint64_t> array;
    std::size_t size = (rrange - lrange) / step + 1; 
    array.reserve(size);

    for (uint64_t i = lrange; i <= rrange; i += step) {
        array.push_back(i);
    }

    return array;
}

// extern "C" void tantivy_log_callback(int level, const char* thread_id, const char* thread_name, const char* message)
// {
//     string threadId(thread_id);
//     string threadName(thread_name);
//     string msg(message);
    
//     switch(level) {
//         case -2: // -2 -> fatal
//             cout << "C++: FATAL [" + threadId + ":" + threadName + "] " + msg << endl;
//             break;
//         case -1: // -1 -> error
//             cout << "C++: ERROR [" + threadId + ":" + threadName + "] " + msg << endl;
//             break;
//         case 0: // 0 -> warning
//             cout << "C++: WARNING [" + threadId + ":" + threadName + "] " + msg << endl;
//             break;
//         case 1: // 1 -> info
//             cout << "C++: INFO [" + threadId + ":" + threadName + "] " + msg << endl;
//             break;
//         case 2: // 2 -> debug
//             cout << "C++: DEBUG [" + threadId + ":" + threadName + "] " + msg << endl;
//             break;
//         case 3: // 3 -> tracing
//             cout << "C++: TRACING [" + threadId + ":" + threadName + "] " + msg << endl;
//             break;
//         default:
//             cout << "C++: DEBUG [" + threadId + ":" + threadName + "] " + msg << endl;
//     }
// }

struct Doc {
    int id;
    std::string title;
    std::string body;
};

// from json to Doc.
void from_json(const json& j, Doc& doc) {
    j.at("id").get_to(doc.id);
    j.at("title").get_to(doc.title);
    j.at("body").get_to(doc.body);
}

size_t index_docs_from_json(const std::string & json_file_path, const std::string & index_path){
    std::ifstream file(json_file_path);

    // parase JSON
    json j;
    file >> j;

    // from json file to Doc vector
    std::vector<Doc> docs = j.get<std::vector<Doc>>();

    TantivySearchIndexW* indexW = tantivy_create_index(index_path.c_str());

    // index all docs
    size_t row_id = 0;
    for (const auto& doc : docs) {
        tantivy_index_doc(indexW, row_id, doc.body.c_str());
        row_id+=1;
    }
    tantivy_writer_commit(indexW);
    tantivy_writer_free(indexW);
    return row_id;
}




void record_nanos_latency(long long latency) {
    std::lock_guard<std::mutex> lock(nano_latencies_mutex);
    nano_latencies.push_back(latency);
}

// 计算百分位数
double calculate_percentile(int percentile) {
    std::lock_guard<std::mutex> lock(nano_latencies_mutex);
    if (nano_latencies.empty()) return 0;

    std::sort(nano_latencies.begin(), nano_latencies.end());
    int index = percentile * nano_latencies.size() / 100;

    double latency_ms = nano_latencies[index] / 1000000.0;
    return std::round(latency_ms * 1000.0) / 1000.0;
}

double calculate_average() {
    std::lock_guard<std::mutex> lock(nano_latencies_mutex);
    if (nano_latencies.empty()) return 0;
    
    double sum = std::accumulate(nano_latencies.begin(), nano_latencies.end(), 0LL);
    double average = sum / nano_latencies.size();
    double average_ms = average / 1000000.0;

    return std::round(average_ms * 1000.0) / 1000.0;
}




int main() {
    auto tantivy_log_callback = [](int level, const char* thread_id, const char* thread_name, const char* message)
    {
        string threadId(thread_id);
        string threadName(thread_name);
        string msg(message);


        switch(level) {
            case -2: // -2 -> fatal
                if (log_level == "fatal" )
                    std::cout << "C++: FATAL [" + threadId + ":" + threadName + "] " + msg << endl;
                break;
            case -1: // -1 -> error
                if (log_level == "error" )
                    std::cout << "C++: ERROR [" + threadId + ":" + threadName + "] " + msg << endl;
                break;
            case 0: // 0 -> warning
                if (log_level == "warning" )
                    std::cout << "C++: WARNING [" + threadId + ":" + threadName + "] " + msg << endl;
                break;
            case 1: // 1 -> info
                if (log_level == "info" )
                    std::cout << "C++: INFO [" + threadId + ":" + threadName + "] " + msg << endl;
                break;
            case 2: // 2 -> debug
                if (log_level == "debug" )
                    std::cout << "C++: DEBUG [" + threadId + ":" + threadName + "] " + msg << endl;
                break;
            case 3: // 3 -> tracing
                if (log_level == "tracing" )
                    std::cout << "C++: TRACING [" + threadId + ":" + threadName + "] " + msg << endl;
                break;
            default:
                std::cout << "C++: DEBUG [" + threadId + ":" + threadName + "] " + msg << endl;
        }
    };





    tantivy_logger_initialize("./log", "info", false, tantivy_log_callback, true, false);
    std::string datasets_path = "/home/mochix/workspace_github/tantivy-search/examples/wiki_560w.json";
    std::string index_path = "/home/mochix/workspace_github/tantivy-search/index_path/";
    std::string query_terms_path = "/home/mochix/workspace_github/tantivy-search/examples/query_terms.json";
    size_t row_id_step = 8192;
    size_t pool_size = 1;
    // parallel=8
    ThreadPool pool(pool_size);
    // 读取 JSON 文件
    std::ifstream file(query_terms_path);
    json j;
    file >> j;
    std::vector<std::string> terms = j["terms"];
    cout << "query terms size:"<< terms.size()<<endl;
    std::atomic<int> query_count(0);







    cout << "begin to index docs from:"<< datasets_path<<endl;
    size_t total_rows = index_docs_from_json(datasets_path, index_path);
    auto row_id_range = generate_array(row_id_step, 0, total_rows);
    cout << "loading index:"<< index_path<<endl;
    TantivySearchIndexR *indexR = tantivy_load_index(index_path.c_str());
    cout << "trying benchmark"<<endl;

    auto start = std::chrono::high_resolution_clock::now();

    std::thread reporter([&]() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(10));
            // calculate average qps
            auto now = std::chrono::high_resolution_clock::now();
            auto total_duration = std::chrono::duration_cast<std::chrono::seconds>(now - start).count();
            int total_queries = query_count.load();
            int qps = total_duration > 0 ? total_queries / static_cast<double>(total_duration) : 0;
            // calculate p/xx
            double p10 = calculate_percentile(10);
            double p30 = calculate_percentile(30);
            double p50 = calculate_percentile(50);
            double p70 = calculate_percentile(70);
            double p99 = calculate_percentile(99);
            double average_latency = calculate_percentile(99);
            std::cout << "queue size: " << pool.getTaskQueueSize() 
                    << ", query_count: " << query_count << "(" << query_count/terms.size() <<")"
                    << ", duration: " << total_duration 
                    << " s || qps: " << qps
                    << std::fixed << std::setprecision(3) 
                    << ", avg latency: " << average_latency 
                    << " ms, p10: " << p10 
                    << " ms, p30: " << p30 
                    << " ms, p50: " << p50 
                    << " ms, p70: " << p70 
                    << " ms, p99: " << p99
                    << " ms" << std::endl;
        }
    });

    while (true) {
        for (size_t i = 0; i < terms.size(); i++) {
            std::string term = terms[i];
            while (pool.getTaskQueueSize()>1000)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            pool.enqueue([term, &row_id_range, row_id_step, &indexR, &query_count]() {
                auto start_query = std::chrono::high_resolution_clock::now();
                for (size_t i = 0; i < row_id_range.size(); i++) {
                    tantivy_search_in_rowid_range(indexR, term.c_str(), row_id_range[i], row_id_range[i] + row_id_step);
                }
                query_count++;
                auto end_query = std::chrono::high_resolution_clock::now();
                long long latency = std::chrono::duration_cast<std::chrono::nanoseconds>(end_query - start_query).count();
                record_nanos_latency(latency);

            });
        }
    }

    reporter.join();

    return 0;

}
