use tantivy::collector::Count;
use tantivy::query::RegexQuery;
use tantivy::schema::{Schema, INDEXED, TEXT, FAST, TextOptions, TextFieldIndexing, IndexRecordOption};
use tantivy::{Index, Document};


fn main() {
    let mut schema_builder = Schema::builder();

    let text_options = TextOptions::default()
        .set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("raw")
                .set_index_option(IndexRecordOption::Basic)
    );

    let row_id = schema_builder.add_u64_field("row_id", FAST|INDEXED);
    let text = schema_builder.add_text_field("text", text_options);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);



    let mut index_writer = index.writer_with_num_threads(8, 1024 * 1024 * 1024).unwrap();

    let str_vec: Vec<String> = vec![
        "Ancient empires rise and fall, shaping 🐶history's course.".to_string(),
        "Artistic expres🐶sio$ns reflect diverse cultural heritages.".to_string(),
        "Social move?ments transform soc>iet不好%你ies, forging new paths.".to_string(),
        "Eco$nomies🐶 fluctuate, % reflecting the complex interplay of global forces.".to_string(),
        r"Strategic military 🐶%🐈 camp%aigns alter the bala🚀nce of power.\%".to_string(),
        ];
    
    for i in 0..str_vec.len() {
        let mut temp = Document::default();
        temp.add_u64(row_id, i as u64);
        temp.add_text(text, &str_vec[i]);
        let _ = index_writer.add_document(temp);
    }
    index_writer.commit().unwrap();

    let reader = index.reader().unwrap();
    let searcher = reader.searcher();

    let cat_regex_query = RegexQuery::from_pattern(".*🐈.*", text).unwrap();
    let bigger_regex_query = RegexQuery::from_pattern(".*\\\\%", text).unwrap();
    let percent_regex_query = RegexQuery::from_pattern(r".*\\%", text).unwrap();

    
    let cat_regex_result = searcher.search(&cat_regex_query, &Count).expect("failed to search");
    let bigger_regex_result = searcher.search(&bigger_regex_query, &Count).expect("failed to search");
    let percent_regex_result = searcher.search(&percent_regex_query, &Count).expect("failed to search");

    println!("cat regex result count:{:?}", cat_regex_result);
    println!("bigger result count:{:?}", bigger_regex_result);
    println!("'%' result count:{:?}", percent_regex_result);
}
