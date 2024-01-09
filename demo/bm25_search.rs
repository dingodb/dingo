use std::sync::Arc;

use roaring::RoaringBitmap;
use tantivy::fastfield::Column;
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, FAST, INDEXED, STORED, TEXT};
use tantivy::{DocAddress, Document, Index, SegmentReader};
use tantivy_search::search::top_dos_with_bitmap_collector::TopDocsWithFilter;

fn main() {
    let mut schema_builder = Schema::builder();
    let row_id = schema_builder.add_u64_field("row_id", FAST | INDEXED);
    let text = schema_builder.add_text_field("text", TEXT | STORED);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());

    let mut index_writer = index
        .writer_with_num_threads(8, 1024 * 1024 * 1024)
        .unwrap();

    let str_vec: Vec<String> = vec![
        "Ancient empires shape history's course".to_string(),
        "New ideas rise, inspiring innovation.".to_string(),
        "Ancient empires rise and fall, shaping history's course.".to_string(),
        "Artistic expressions reflect diverse cultural heritages.".to_string(),
        "Social movements transform societies, forging new paths.".to_string(),
        "Economies fluctuate, reflect the complex interplay of global forces, and reflect me also"
            .to_string(),
        "Economies fluctuate, reflect the complex interplay of global forces".to_string(),
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
    row_id_bitmap.insert(1);
    row_id_bitmap.insert(2);
    row_id_bitmap.insert(3);
    row_id_bitmap.insert(5);
    let top_docs: Vec<(f32, DocAddress)> = searcher
        .search(
            &query,
            &TopDocsWithFilter::with_limit(10).and_row_id_bitmap(Arc::new(row_id_bitmap)),
        )
        .unwrap();
    for (score, doc_address) in top_docs {
        let retrieved_doc = searcher.doc(doc_address).unwrap();
        let fast_field_reader: &Column<u64> = &fast_field_readers[doc_address.segment_ord as usize];
        let row_id = fast_field_reader
            .values_for_doc(doc_address.doc_id)
            .next()
            .unwrap();
        println!(
            "row_id:{}, score:{}, doc:{}",
            row_id,
            score,
            schema.to_json(&retrieved_doc)
        );
    }
}
