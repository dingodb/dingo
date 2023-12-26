use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering, AtomicBool};
use std::time::{Instant, Duration};
use std::{fs::File, io::BufReader, ffi::CString};
use clap::{App, Arg};
use serde_json::Value;
use tantivy_search::{tantivy_create_index, tantivy_index_doc, tantivy_search_in_rowid_range, tantivy_load_index, tantivy_reader_free};
use tantivy_search::utils::IndexR;
use threadpool::ThreadPool;
use std::thread;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::process;

#[derive(Serialize, Deserialize)]
struct Doc {
    id: i32,
    title: String,
    body: String,
}

fn index_docs_from_json(json_file_path: &str, index_path: &str) -> usize {
    let c_index_path = CString::new(index_path).expect("Can't create CString with index_path");
    let c_index_path_ptr = c_index_path.as_ptr();
    let iw = tantivy_create_index(c_index_path_ptr);

    // Read JSON and parse into Vec<Doc>
    let file = File::open(json_file_path).expect("Json file not found");
    let reader = BufReader::new(file);
    let docs: Vec<Doc> = serde_json::from_reader(reader).expect("Error parning JSON");
    
    // Create and use Tantivy index
    let mut count= 0;
    for doc in docs {
        let c_doc = CString::new(doc.body).expect("Can't create CString with doc.body");
        let c_doc_ptr = c_doc.as_ptr();
        tantivy_index_doc(iw, count, c_doc_ptr);
        count+=1;
    }
    return count as usize;
}

fn calculate_percentile(latencies: Arc<Mutex<Vec<u128>>>, percentile: usize) -> String {
    // Calculate percentile
    let mut latencies = latencies.lock().unwrap();
    if latencies.is_empty() {
        return "0.000".to_string();
    }
    latencies.sort_unstable();
    let index = percentile * latencies.len() / 100;
    let latency_ns = latencies[index];
    // convert ns -> ms
    format!("{:.3}", latency_ns as f64 / 1_000_000.0)
}

fn calculate_average(latencies: Arc<Mutex<Vec<u128>>>) -> String {
    // Calculate average
    let latencies = latencies.lock().unwrap();
    if latencies.is_empty() {
        return "0.00".to_string();
    }
    let sum: u128 = latencies.iter().sum();
    let average = sum as f64 / latencies.len() as f64;
    // convert ns -> ms
    format!("{:.3}", average as f64 / 1_000_000.0)
}

fn run_benchmark(terms: &[String], 
                duration_seconds: usize, 
                pool: &ThreadPool,
                index_r: Arc<IndexR>,
                query_count: Arc<AtomicUsize>, 
                row_id_range: &[usize], 
                row_id_step: usize,
                latencies: Arc<Mutex<Vec<u128>>>,
                pool_max_active_count: usize) {
    // Benchmark logic
    let benchmark_start_time = Instant::now();

    while Instant::now().duration_since(benchmark_start_time) < Duration::from_secs(duration_seconds as u64) {
        for term in terms.iter() {
            // 限制 pool 队列等待的最大长度
            while pool.queued_count()>=pool_max_active_count {
                thread::sleep(Duration::from_millis(1000));
            }
            // 超过限制时间, 停止执行
            if Instant::now().duration_since(benchmark_start_time) > Duration::from_secs(duration_seconds as u64) {
                break;
            }
            let term_clone = term.clone();
            let row_id_range_clone = row_id_range.to_vec();
            let query_count_clone = Arc::clone(&query_count);
            let index_r_clone = Arc::clone(&index_r); // 克隆 Arc<IndexR>
            let latencies_clone = Arc::clone(&latencies);
            pool.execute(move || {
                let start_query = Instant::now();
                let mut hitted = 0;
                
                // 将 Arc 转换为裸指针
                let index_r_ptr = Arc::into_raw(index_r_clone) as *mut IndexR;
                for &start in row_id_range_clone.iter() {
                    let end = start + row_id_step;
                    let c_term = CString::new(term_clone.clone()).expect("Can't create CString with term_clone");
                    let c_term_ptr = c_term.as_ptr();
                    if tantivy_search_in_rowid_range(index_r_ptr, c_term_ptr, start as u64, end as u64, false) {
                        hitted+=1;
                    }
                }
                // 重新获得 Arc 所有权, 防止内存泄漏
                let temp_arc = unsafe { Arc::from_raw(index_r_ptr) };
                query_count_clone.fetch_add(1, Ordering::SeqCst);
                let end_query = Instant::now();
                let latency = end_query.duration_since(start_query).as_nanos();
                // println!("{:?} hitted, arc:{:?}, time elapsed {:?} ms", hitted, Arc::strong_count(&temp_arc), format!("{:.3}", (latency as f64) / 1_000_000.0));
                add_latency(latencies_clone, latency);
            });
        }
    }
}

// add data to latencies
fn add_latency(latencies: Arc<Mutex<Vec<u128>>>, value: u128) {
    let mut latencies = latencies.lock().unwrap();
    latencies.push(value);
    // println!("latencies size: {:?}", latencies.len());
}

fn generate_array(step: usize, lrange: usize, rrange: usize) -> Vec<usize> {
    (lrange..=rrange).step_by(step).collect()
}

fn load_terms_from_json<P: AsRef<Path>>(path: P) -> Vec<String> {
    let file = File::open(path).expect("Unable to open the file");
    let reader = BufReader::new(file);
    let json: Value = serde_json::from_reader(reader).expect("Unable to parse JSON");

    // 假设 JSON 结构是 {"terms": ["term1", "term2", ...]}
    json["terms"].as_array().expect("Expected an array")
        .iter()
        .map(|val| val.as_str().expect("Expected a string").to_string())
        .collect()
}

fn main() {
    println!("After 10s, program will start");
    let pid = process::id();
    println!("##### PID: {}", pid);
    // thread::sleep(Duration::from_secs(10));

    // parse args
    let matches = App::new("tantivy-search-benchmark")
        .version("1.0")
        .author("mochix")
        .about("Do some benchmarking")
        .arg(Arg::with_name("pool-size")
            .short("ps")
            .long("pool-size")
            .value_name("POOL_SIZE")
            .help("Set pool size")
            .default_value("1"))
        .arg(Arg::with_name("each-benchmark-duration")
            .short("ebd")
            .long("each-benchmark-duration")
            .value_name("EACH_BENCHMARK_DURATION")
            .help("Set each benchmark duration")
            .default_value("1200")) // default 20min
        .arg(Arg::with_name("skip-build-index")
            .short("sbi")
            .long("skip-build-index")
            .value_name("SKIP_BUILD_INDEX")
            .help("Skip build index")
            .default_value("true"))
        .arg(Arg::with_name("skip-benchmark")
            .short("sb")
            .long("skip-benchmark")
            .value_name("SKIP_BENCHMARK")
            .help("Skip benchmark")
            .default_value("false"))
        .arg(Arg::with_name("each-free-wait")
            .short("efw")
            .long("each-free-wait")
            .value_name("EACH_FREE_WAIT")
            .help("Set each free operation wait time")
            .default_value("120"))
        .arg(Arg::with_name("iter-times")
            .short("it")
            .long("iter-times")
            .value_name("ITER_TIMES")
            .help("Set iter times")
            .default_value("30"))
        .arg(Arg::with_name("index-path")
            .short("ip")
            .long("index-path")
            .value_name("INDEX_PATH")
            .help("Set index path")
            .default_value("/home/mochix/tantivy_search_memory/index_path"))
        .arg(Arg::with_name("query-term-path")
            .short("qtp")
            .long("query-term-path")
            .value_name("QUERY_TERM_PATH")
            .help("Set query term path")
            .default_value("/home/mochix/workspace_github/tantivy-search/examples/query_terms.json"))
        .arg(Arg::with_name("dataset-path")
            .short("dp")
            .long("dataset-path")
            .value_name("DATASET_PATH")
            .help("Set dataset path")
            .default_value("/home/mochix/workspace_github/tantivy-search/examples/wiki_560w.json"))
        .arg(Arg::with_name("max-task-size")
            .short("mts")
            .long("max-task-size")
            .value_name("MAX_TASK_SIZE")
            .help("Set max task size")
            .default_value("1000"))
        .get_matches();

    let pool_size: usize = matches.value_of("pool-size")
        .and_then(|v| v.parse().ok())
        .expect("Invalid value for pool size");
    let each_benchmark_duration: usize = matches.value_of("each-benchmark-duration")
        .and_then(|v| v.parse().ok())
        .expect("Invalid value for each benchmark duration");
    let skip_build_index: bool = matches.value_of("skip-build-index")
        .and_then(|v| v.parse().ok())
        .expect("Invalid value for skip build index");
    let skip_benchmark: bool = matches.value_of("skip-benchmark")
        .and_then(|v| v.parse().ok())
        .expect("Invalid value for skip benchmark");
    let each_free_wait: u64 = matches.value_of("each-free-wait")
        .and_then(|v| v.parse().ok())
        .expect("Invalid value for each free wait");
    let iter_times: i32 = matches.value_of("iter-times")
        .and_then(|v| v.parse().ok())
        .expect("Invalid value for iter times");
    let index_path = matches.value_of("index-path").expect("Index path not provided");
    let query_term_path = matches.value_of("query-term-path").expect("Query term path not provided");
    let dataset_path = matches.value_of("dataset-path").expect("Dataset path not provided");
    let max_task_size: usize = matches.value_of("max-task-size")
        .and_then(|v| v.parse().ok())
        .expect("Invalid value for max task size");

    println!("##### Pool Size: {}", pool_size);
    println!("##### Each Benchmark Duration: {}", each_benchmark_duration);
    println!("##### Skip Build Index: {}", skip_build_index);
    println!("##### Skip Benchmark: {}", skip_benchmark);
    println!("##### Each Free Wait: {}", each_free_wait);
    println!("##### Iter Times: {}", iter_times);
    println!("##### Index Path: {}", index_path);
    println!("##### Query Term Path: {}", query_term_path);
    println!("##### Dataset Path: {}", dataset_path);
    println!("##### Max Task Size: {}", max_task_size);

    // Setup (parsing args, setting up thread pool, etc.)
    let query_count = Arc::new(AtomicUsize::new(0));
    let latencies = Arc::new(Mutex::new(Vec::<u128>::new()));
    let is_finished = Arc::new(AtomicBool::new(false));
    let mut total_rows = 5600000;
    let row_id_step = 8192;


    let start = Instant::now();
    let reporter_query_count = Arc::clone(&query_count);
    let reporter_latencies = Arc::clone(&latencies);
    let reporter_is_finished = Arc::clone(&is_finished);
    let reporter_thread = thread::spawn(move||{
        while !reporter_is_finished.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_secs(10));
            let total_duration = start.elapsed().as_secs();
            let total_queries = reporter_query_count.load(Ordering::SeqCst);
            let qps = if total_duration > 0 {
                let qps_value = total_queries as f64 / total_duration as f64;
                format!("{:.2}", qps_value)
            } else {
                "0.0".to_string()
            };
            let p10 = calculate_percentile(Arc::clone(&reporter_latencies), 10);
            let p30 = calculate_percentile(Arc::clone(&reporter_latencies), 30);
            let p50 = calculate_percentile(Arc::clone(&reporter_latencies), 50);
            let p70 = calculate_percentile(Arc::clone(&reporter_latencies), 70);
            let p99 = calculate_percentile(Arc::clone(&reporter_latencies), 99);
            let average_latency = calculate_average(Arc::clone(&reporter_latencies));
    
            println!("QPS: {}, Avg Latency: {} ms, P10: {} ms, P30: {} ms, P50: {} ms, P70: {} ms, P99: {} ms", qps, average_latency, p10, p30, p50, p70, p99);
            
        }
    });

    // 索引数据集
    if !skip_build_index {
        println!("Starting index docs from dataset: {:?}", dataset_path);
        total_rows = index_docs_from_json(dataset_path, index_path);
        println!("{:?} docs has been indexed.", total_rows);

    }
    // 初始化相关数据
    let terms = load_terms_from_json(query_term_path);
    let row_id_range = generate_array(8192, 0, total_rows);
    let pool = ThreadPool::new(pool_size);


    println!("Starting benchmark, pool: {}", pool_size);

    for i in 0..iter_times {
        println!("[{}] Loading tantivy index from index_path after {}s", i, each_free_wait);
        thread::sleep(Duration::from_secs(each_free_wait));
        println!("[{}] Waiting finished.", i);

        let c_index_path = CString::new(index_path).expect("Can't create CString with index_path");
        let c_index_path_ptr = c_index_path.as_ptr();

        let index_r: *mut IndexR = tantivy_load_index(c_index_path_ptr); // 加载索引, 获得指向一块内存区域的裸指针
        let index_r_box: Box<IndexR> = unsafe { Box::from_raw(index_r) }; // Box 获得裸指针指向的内存区域
        let index_r_arc = Arc::new(*index_r_box); // 解引用 Box 所有的 IndexR 对象给 Arc, 所有权发生 move

        // 运行基准测试
        println!("[{}] Trying benchmark, index_r_arc:{:?}", i, Arc::strong_count(&index_r_arc));
        if !skip_benchmark {
            run_benchmark(&terms, 
                each_benchmark_duration, 
                &pool, 
                Arc::clone(&index_r_arc),
                Arc::clone(&query_count), 
                &row_id_range, 
                row_id_step, 
                Arc::clone(&latencies),
                max_task_size);   
        }
        // Waiting queue clean
        while pool.queued_count() > 0 {
            println!("[{}] Waiting queue-[{}] empty...", i, pool.queued_count());
            thread::sleep(Duration::from_secs(5));
        }
        println!("[{}] Waiting queue-[{}] done.", i, pool.queued_count());
        println!("[{}] Trying auto free tantivy index reader, index_r_arc:{:?}", i, Arc::strong_count(&index_r_arc));
        
        // Arc 引用计数 = 1, 自动释放 IndexR
    }

    is_finished.store(true, Ordering::SeqCst);
    reporter_thread.join().unwrap();

    println!("Waiting 10s stop this program");
    thread::sleep(Duration::from_secs(10));
}
