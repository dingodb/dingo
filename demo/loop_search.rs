use cxx::let_cxx_string;
use rand::seq::SliceRandom;
use rand::thread_rng;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
use std::thread;
use std::time::Duration;
use std::{fs::File, io::BufReader};
use tantivy::collector::Count;
use tantivy::query::QueryParser;
use tantivy::{Index, ReloadPolicy};
use tantivy_search::index::index_manager::{tantivy_create_index, tantivy_index_doc};

#[derive(Serialize, Deserialize)]
struct Doc {
    id: i32,
    title: String,
    body: String,
}

fn index_docs_from_json(json_file_path: &str, index_path: &str) -> usize {
    let_cxx_string!(index_path_cxx = index_path);
    let index_path_cxx = index_path_cxx.as_ref().get_ref();
    let _ = tantivy_create_index(index_path_cxx);

    // Read JSON and parse into Vec<Doc>
    let file = File::open(json_file_path).expect("Json file not found");
    let reader = BufReader::new(file);
    let docs: Vec<Doc> = serde_json::from_reader(reader).expect("Error parning JSON");

    // Create and use Tantivy index
    let mut count = 0;
    for doc in docs {
        let_cxx_string!(doc_cxx = doc.body);
        let doc_cxx = doc_cxx.as_ref().get_ref();
        let _ = tantivy_index_doc(index_path_cxx, count, doc_cxx);
        count += 1;
    }
    return count as usize;
}

// fn generate_array(step: usize, lrange: usize, rrange: usize) -> Vec<usize> {
//     (lrange..=rrange).step_by(step).collect()
// }

fn load_terms_from_json<P: AsRef<Path>>(path: P) -> Vec<String> {
    let file = File::open(path).expect("Unable to open the file");
    let reader = BufReader::new(file);
    let json: Value = serde_json::from_reader(reader).expect("Unable to parse JSON");

    // 假设 JSON 结构是 {"terms": ["term1", "term2", ...]}
    json["terms"]
        .as_array()
        .expect("Expected an array")
        .iter()
        .map(|val| val.as_str().expect("Expected a string").to_string())
        .collect()
}

fn main() {
    let each_loop_terms_size = 1000; // each loop search counts.
    let free_seconds: u64 = 120; // waiting before free resource
    let query_terms_path = "query_terms.json";
    let index_path = "index_files_path";
    let dataset_path = "wiki_560w.json";
    let skip_index_build = true;
    let loop_size = 100;

    // 索引数据集
    if !skip_index_build {
        println!("Starting index docs from dataset: {:?}", dataset_path);
        index_docs_from_json(dataset_path, index_path);
    }
    let terms = load_terms_from_json(query_terms_path);

    println!(
        "Starting loop search, loop_size: {}, each_loop_terms_size: {}",
        loop_size, each_loop_terms_size
    );

    for i in 0..loop_size {
        println!(
            "[{}] Loading tantivy index from index_path after {}s",
            i, free_seconds
        );
        thread::sleep(Duration::from_secs(free_seconds));

        let index = Index::open_in_dir(index_path).expect("Can't load index");
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()
            .expect("Can't init reader");

        let mut rng = thread_rng();
        let mut result_sum = 0;

        // search `each_loop_terms_size` terms.
        for _ in 0..each_loop_terms_size {
            if let Some(random_choice) = terms.choose(&mut rng) {
                let schema = index.schema();
                let text = schema.get_field("text").expect("Can't get text field");
                let searcher = reader.searcher();
                let query_parser = QueryParser::for_index(&index, vec![text]);
                let text_query = query_parser
                    .parse_query(random_choice)
                    .expect("Can't parse query.");
                let count = searcher.search(&text_query, &Count).expect("Can't search");
                result_sum += count;
            }
        }
        // drop(index);
        // drop(reader);
        println!(
            "[{}] searched {} terms, total count:{:?}",
            i, each_loop_terms_size, result_sum
        );
    }
}
