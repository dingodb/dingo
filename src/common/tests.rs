use std::{cmp::min, sync::Arc};

use tantivy::{collector::Count, merge_policy::LogMergePolicy, query::QueryParser, schema::{Schema, FAST, INDEXED, TEXT}, Index, IndexReader, IndexWriter, TantivyDocument};

use crate::{index::bridge::index_writer_bridge::IndexWriterBridge, FFI_INDEX_WRITER_CACHE};

#[allow(dead_code)]
pub fn get_mocked_docs() -> (Vec<String>, Vec<String>, Vec<String>) {
    let col1_docs: Vec<String> = vec![
        "Ancient empires rise and fall, shaping history's course.".to_string(),
        "Artistic expressions reflect diverse cultural heritages.".to_string(),
        "Social movements transform societies, forging new paths.".to_string(),
        "Strategic military campaigns alter the balance of power.".to_string(),
        "Ancient philosophies provide wisdom for modern dilemmas.".to_string(),
    ];
    let col2_docs: Vec<String> = vec![
        "Brave explorers venture into uncharted territories, expanding horizons.".to_string(),
        "Brilliant minds unravel nature's judgment through scientific inquiry.".to_string(),
        "Economic systems evolve, influencing global trade and prosperity.".to_string(),
        "Environmental challenges demand innovative solutions for sustainability.".to_string(),
        "Ethical dilemmas test the boundaries of moral reasoning and Judgment.".to_string(),
    ];

    let col3_docs: Vec<String> = vec![
        "Groundbreaking inventions revolutionize industries and daily life.".to_string(),
        "Iconic leaders inspire generations with their vision and charisma.".to_string(),
        "Literary masterpieces capture the essence of the human experience.".to_string(),
        "Majestic natural wonders showcase the breathtaking beauty of Earth.".to_string(),
        "Philosophical debates shape our understanding of reality and existence.".to_string(),
    ];
    return (col1_docs, col2_docs, col3_docs);
}

#[allow(dead_code)]
fn create_3column_schema() -> Schema {
    // Construct the schema for the index.
    let mut schema_builder = Schema::builder();
    schema_builder.add_u64_field("row_id", FAST | INDEXED);
    schema_builder.add_text_field("col1", TEXT);
    schema_builder.add_text_field("col2", TEXT);
    schema_builder.add_text_field("col3", TEXT);
    let schema = schema_builder.build();
    return schema;
}

#[allow(dead_code)]
fn create_3column_index(index_directory: &str) -> (Index, Schema) {
    let schema = create_3column_schema();
    // Create the index in the specified directory.
    let index = Index::create_in_dir(index_directory, schema.clone()).unwrap();
    (index, schema)
}

#[allow(dead_code)]
fn index_documents(writer: &mut IndexWriter, schema: &Schema, col1_docs: &[String], col2_docs: &[String], col3_docs: &[String]) {
    // Get fields from `schema`.
    let row_id_field = schema.get_field("row_id").unwrap();
    let col1_field = schema.get_field("col1").unwrap();
    let col2_field = schema.get_field("col2").unwrap();
    let col3_field = schema.get_field("col3").unwrap();

    // Index some documents.
    for row_id in 0..min(min(col1_docs.len(), col2_docs.len()), col3_docs.len()) {
        let mut doc = TantivyDocument::default();
        doc.add_u64(row_id_field, row_id as u64);
        doc.add_text(col1_field, &col1_docs[row_id]);
        doc.add_text(col2_field, &col2_docs[row_id]);
        doc.add_text(col3_field, &col3_docs[row_id]);
        let result = writer.add_document(doc);
        assert!(result.is_ok());
    }
}

#[allow(dead_code)]
pub fn index_3column_docs_with_threads_merge(index_directory: &str) -> (IndexReader, Schema) {
    let (index, schema) = create_3column_index(index_directory);

    // Create the writer with a specified buffer size (e.g., 64 MB).
    let mut writer = index.writer_with_num_threads(2, 1024 * 1024 * 64).unwrap();
    // Configure default merge policy.
    writer.set_merge_policy(Box::new(LogMergePolicy::default()));

    // Index some documents.
    let (col1_docs, col2_docs, col3_docs) = get_mocked_docs();
    index_documents(&mut writer, &schema, &col1_docs, &col2_docs, &col3_docs);

    assert!(writer.commit().is_ok());
    assert!(writer.wait_merging_threads().is_ok());

    (index.reader().unwrap(), schema)
}

#[allow(dead_code)]
pub fn index_3column_docs_without_threads_merge(index_directory: &str) -> (IndexWriter, IndexReader, Schema) {
    let (index, schema) = create_3column_index(index_directory);

    // Create the writer with a specified buffer size (e.g., 64 MB).
    let mut writer = index.writer_with_num_threads(2, 1024 * 1024 * 64).unwrap();
    // Configure default merge policy.
    writer.set_merge_policy(Box::new(LogMergePolicy::default()));

    // Index some documents.
    let (col1_docs, col2_docs, col3_docs) = get_mocked_docs();
    index_documents(&mut writer, &schema, &col1_docs, &col2_docs, &col3_docs);

    assert!(writer.commit().is_ok());

    (writer, index.reader().unwrap(), schema)
}

#[allow(dead_code)]
pub fn index_3column_docs_with_index_writer_bridge(index_directory: &str, waiting_merging_threads_finished: bool) -> Arc<IndexWriterBridge> {
    // Get index writer from CACHE
    let index_writer_bridge = FFI_INDEX_WRITER_CACHE
            .get_index_writer_bridge(index_directory.to_string())
            .unwrap();
    let (col1_docs, col2_docs, col3_docs) = get_mocked_docs();
    let schema = create_3column_schema();

    // Get fields from `schema`.
    let row_id_field = schema.get_field("row_id").unwrap();
    let col1_field = schema.get_field("col1").unwrap();
    let col2_field = schema.get_field("col2").unwrap();
    let col3_field = schema.get_field("col3").unwrap();


    for row_id in 0..min(min(col1_docs.len(), col2_docs.len()), col3_docs.len()) {
        let mut doc = TantivyDocument::default();
        doc.add_u64(row_id_field, row_id as u64);
        doc.add_text(col1_field, &col1_docs[row_id]);
        doc.add_text(col2_field, &col2_docs[row_id]);
        doc.add_text(col3_field, &col3_docs[row_id]);
        let result = index_writer_bridge.add_document(doc);
        assert!(result.is_ok());
    }
    assert!(index_writer_bridge.commit().is_ok());
    if waiting_merging_threads_finished {
        assert!(index_writer_bridge.wait_merging_threads().is_ok());
    }

    index_writer_bridge
}

#[allow(dead_code)]
pub fn search_with_index_writer_bridge(index_writer_bridge: Arc<IndexWriterBridge>) {
    // Get fields from `schema`.
    let schema = index_writer_bridge.index.schema();
    let col1_field = schema.get_field("col1").unwrap();
    let col2_field = schema.get_field("col2").unwrap();
    let col3_field = schema.get_field("col3").unwrap();
    let parser_col1 = QueryParser::for_index(&index_writer_bridge.index, vec![col1_field]);
    let parser_col2 = QueryParser::for_index(&index_writer_bridge.index, vec![col2_field]);
    let parser_col3 = QueryParser::for_index(&index_writer_bridge.index, vec![col3_field]);
    let parser_all = QueryParser::for_index(
        &index_writer_bridge.index,
        vec![col1_field, col2_field, col3_field],
    );

    let text_query_in_col1 = parser_col1.parse_query("of").unwrap();
    let text_query_in_col2 = parser_col2.parse_query("of").unwrap();
    let text_query_in_col3 = parser_col3.parse_query("of").unwrap();
    let text_query_in_all = parser_all.parse_query("of").unwrap();

    // Test whether index can be use.
    let searcher = index_writer_bridge.index.reader().unwrap().searcher();
    let count_1 = searcher.search(&text_query_in_col1, &Count).unwrap();
    let count_2 = searcher.search(&text_query_in_col2, &Count).unwrap();
    let count_3 = searcher.search(&text_query_in_col3, &Count).unwrap();
    let count_a = searcher.search(&text_query_in_all, &Count).unwrap();

    assert_eq!(count_1, 1);
    assert_eq!(count_2, 1);
    assert_eq!(count_3, 3);
    assert_eq!(count_a, 3);
}