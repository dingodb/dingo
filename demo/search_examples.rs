use std::sync::Arc;

use roaring::RoaringBitmap;
use tantivy::collector::Count;
use tantivy::fastfield::Column;
use tantivy::query::{QueryParser, RegexQuery};
use tantivy::schema::{IndexRecordOption, Schema, TextFieldIndexing, TextOptions, FAST, INDEXED, STORED, TEXT};
use tantivy::{DocAddress, Document, Index, SegmentReader};
use tantivy_search::ffi::RowIdWithScore;
use tantivy_search::search::top_dos_with_bitmap_collector::TopDocsWithFilter;

// Build index and return index reader.
fn build_index_in_ram(schema: Schema) -> Index {
    let text = schema.get_field("text").expect("schema doesn't have `text`.");
    let row_id = schema.get_field("row_id").expect("schema doesn't have `row_id`.");

    let index = Index::create_in_ram(schema);
    let mut index_writer = index
        .writer_with_num_threads(2, 1024 * 1024 * 32)
        .unwrap();
    let str_vec: Vec<String> = vec![
        "Ancient empires rise and fall, shaping history's course.".to_string(),
        "Artistic expres🐶sio$ns reflect diverse cultural🐈 heritages.".to_string(),
        "Social move?ments transform soc>iet不好%你ies, forging new paths.".to_string(),
        "Economies fluctuate, reflecting the complex interplay of global forces.".to_string(),
        r"Strategic military campaigns alter the balance of power.\\%".to_string(),
        r"The ocean reflect the gloomy sky.s".to_string(),
    ];
    for i in 0..str_vec.len() {
        let mut temp = Document::default();
        temp.add_u64(row_id, i as u64);
        temp.add_text(text, &str_vec[i]);
        let _ = index_writer.add_document(temp);
    }
    index_writer.commit().unwrap();

    index
}

// demo for regex search
fn regex_search_demo() {
    let mut schema_builder = Schema::builder();

    // Self-customized filed options
    let text_options = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("raw")
            .set_index_option(IndexRecordOption::Basic),
    );

    let _ = schema_builder.add_u64_field("row_id", FAST | INDEXED);
    let text = schema_builder.add_text_field("text", text_options);
    let schema = schema_builder.build();

    let searcher = build_index_in_ram(schema.clone()).reader().unwrap().searcher();

    let cat = RegexQuery::from_pattern(".*🐈.*", text).unwrap();
    let double_backslash = RegexQuery::from_pattern(".*\\\\%", text).unwrap();
    let double_backslash_r = RegexQuery::from_pattern(r".*\\%", text).unwrap();

    let cat_result = searcher
        .search(&cat, &Count)
        .expect("failed to search");
    let double_backslash_result = searcher
        .search(&double_backslash, &Count)
        .expect("failed to search");
    let double_backslash_r_result = searcher
        .search(&double_backslash_r, &Count)
        .expect("failed to search");

    println!("cat count:{:?}", cat_result);
    println!("double_backslash count:{:?}", double_backslash_result);
    println!("double_backslash_r count:{:?}", double_backslash_r_result);
}


// demo for natural language search
fn natural_language_demo() {
    let mut schema_builder = Schema::builder();
    let _ = schema_builder.add_u64_field("row_id", FAST | INDEXED);
    let text = schema_builder.add_text_field("text", TEXT);

    let index = build_index_in_ram(schema_builder.build());
    let query_parser = QueryParser::for_index(&index, vec![text]);

    let natural_query = query_parser
        .parse_query("(\"Ancient empires\" AND rise) OR \"balance of power\"")
        .expect("Can't parse query.");

    let result = index.reader().unwrap().searcher()
        .search(&natural_query, &Count)
        .expect("failed to search");

    println!("natural language query count:{:?}", result);
}


// demo for bm25 search
fn bm25_search_demo() {
    let mut schema_builder = Schema::builder();
    let _ = schema_builder.add_u64_field("row_id", FAST | INDEXED);
    let text = schema_builder.add_text_field("text", TEXT | STORED);
    let schema = schema_builder.build();
    let index = build_index_in_ram(schema.clone());

    let searcher = index.reader().unwrap().searcher();
    let fast_field_readers: Vec<Column<u64>> = searcher
        .segment_readers()
        .iter()
        .map(|segment_reader: &SegmentReader| {
            let fast_fields: &tantivy::fastfield::FastFieldReaders = segment_reader.fast_fields();
            fast_fields.u64("row_id").unwrap()
        })
        .collect();

    let query_parser = QueryParser::for_index(&index, vec![text]);

    let query = query_parser.parse_query("reflect").unwrap();

    let mut row_id_bitmap = RoaringBitmap::new();
    row_id_bitmap.insert(5);

    let top_docs: Vec<RowIdWithScore> = searcher
        .search(
            &query,
            &TopDocsWithFilter::with_limit(10)
                .with_alive(Arc::new(row_id_bitmap))
                .with_searcher(searcher.clone())
                .with_text_field(text)
                .with_stored_text(true),
        )
        .unwrap();
    for row_id_with_score in top_docs {
        let doc_address = DocAddress {
            segment_ord: row_id_with_score.seg_id,
            doc_id: row_id_with_score.doc_id,
        };
        // You can retrive origin doc by doc address.
        let retrieved_doc: Document = searcher.doc(doc_address).unwrap();

        let fast_field_reader: &Column<u64> = &fast_field_readers[doc_address.segment_ord as usize];
        let row_id = fast_field_reader
            .values_for_doc(doc_address.doc_id)
            .next()
            .unwrap();
        println!(
            "row_id:{}, score:{}, doc_id:{}, seg_id:{}, doc:{:?}, retrieved_doc:{:?}, retrieved_doc_to_json:{:?}",
            row_id,
            row_id_with_score.score,
            row_id_with_score.doc_id,
            row_id_with_score.seg_id,
            row_id_with_score.doc,
            retrieved_doc.get_first(text).unwrap().as_text().unwrap(),
            schema.to_json(&retrieved_doc)
        );
    }
}

fn main() {
    regex_search_demo();
    natural_language_demo();
    bm25_search_demo();
}

