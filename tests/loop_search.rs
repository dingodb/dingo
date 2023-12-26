use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering, AtomicBool};
use std::time::{Instant, Duration};
use std::{fs::File, io::BufReader, ffi::CString};
use rand::seq::SliceRandom;
use rand::thread_rng;
use serde_json::Value;
use tantivy::collector::Count;
use tantivy::query::QueryParser;
use tantivy::{Index, ReloadPolicy};
use tantivy_search::{tantivy_create_index, tantivy_index_doc, tantivy_search_in_rowid_range, tantivy_load_index, tantivy_reader_free};
use tantivy_search::utils::IndexR;
use threadpool::ThreadPool;
use std::thread;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

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
    let each_loop_terms_size = 1000; // 每个 loop 搜索的 term 数量
    let free_seconds: u64 = 120; // 释放资源前的等待时间
    let query_terms_path = "/home/mochix/tantivy_search_memory/query_terms.json";
    let index_path = "/home/mochix/tantivy_search_memory/loop_search/index_path";
    let dataset_path = "/home/mochix/tantivy_search_memory/wiki_560w.json";
    let skip_index_build =true;
    let loop_size = 100;

    // 索引数据集
    if !skip_index_build {
        println!("Starting index docs from dataset: {:?}", dataset_path);
        index_docs_from_json(dataset_path, index_path);

    }
    let terms = load_terms_from_json(query_terms_path);

    println!("Starting loop search, loop_size: {}, each_loop_terms_size: {}", loop_size, each_loop_terms_size);

    for i in 0..loop_size {
        println!("[{}] Loading tantivy index from index_path after {}s", i, free_seconds);
        thread::sleep(Duration::from_secs(free_seconds));

        let index = Index::open_in_dir(index_path).expect("Can't load index");
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into().expect("Can't init reader");

        let mut rng = thread_rng();
        let mut result_sum  = 0;

        // search `each_loop_terms_size` terms.
        for _ in 0..each_loop_terms_size {
            if let Some(random_choice) = terms.choose(&mut rng) {
                let schema = index.schema();
                let text = schema.get_field("text").expect("Can't get text field");
                let searcher = reader.searcher();
                let query_parser = QueryParser::for_index(&index, vec![text]);
                let text_query = query_parser.parse_query(random_choice).expect("Can't parse query.");
                let count = searcher.search(&text_query,&Count).expect("Can't search");
                result_sum += count;
            }
        }
        // drop(index);
        // drop(reader);
        println!("[{}] searched {} terms, total count:{:?}", i, each_loop_terms_size, result_sum);
    }
}
