use tantivy::collector::Count;
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, FAST, INDEXED, TEXT};
use tantivy::{Document, Index};

fn main() {
    let mut schema_builder = Schema::builder();
    let row_id = schema_builder.add_u64_field("row_id", FAST | INDEXED);
    let text = schema_builder.add_text_field("text", TEXT);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);

    let mut index_writer = index
        .writer_with_num_threads(8, 1024 * 1024 * 1024)
        .unwrap();

    let str_vec: Vec<String> = vec![
        "Ancient empires shape history's course".to_string(),
        "New ideas rise, inspiring innovation.".to_string(),
        "Ancient empires rise and fall, shaping history's course.".to_string(),
        "Artistic expressions reflect diverse cultural heritages.".to_string(),
        "Social movements transform societies, forging new paths.".to_string(),
        "Economies fluctuate, reflecting the complex interplay of global forces.".to_string(),
        r"Strategic military campaigns alter the balance of power.".to_string(),
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

    let query_parser = QueryParser::for_index(&index, vec![text]);
    // let text_query = query_parser.parse_query("(empires AND rise) OR \"balance of power\"").expect("Can't parse query.");
    let text_query = query_parser
        .parse_query("(\"Ancient empires\" AND rise) OR \"balance of power\"")
        .expect("Can't parse query.");
    let result = searcher
        .search(&text_query, &Count)
        .expect("failed to search");

    println!("result count:{:?}", result);
}
