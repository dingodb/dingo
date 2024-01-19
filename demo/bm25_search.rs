use std::sync::Arc;

use roaring::RoaringBitmap;
use tantivy::fastfield::Column;
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, FAST, INDEXED, STORED, TEXT};
use tantivy::{DocAddress, Document, Index, SegmentReader};
use tantivy_search::ffi::RowIdWithScore;
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
        "1- Ancient empires shape history's course".to_string(),
        "2- Ancient empires shape history's course".to_string(),
        "3- New ideas rise, inspiring innovation.".to_string(),
        "4- New ideas rise, inspiring innovation.".to_string(),
        "5- Ancient empires rise and fall, shaping history's course.".to_string(),
        "6- Ancient empires rise and fall, shaping history's course.".to_string(),
        "7- Artistic expressions reflect diverse cultural heritages.".to_string(),
        "8- Artistic expressions reflect diverse cultural heritages.".to_string(),
        "9- Social movements transform societies, forging new paths.".to_string(),
        "10- Social movements transform societies, forging new paths.".to_string(),
        "11- Economies fluctuate, reflect the complex interplay of global forces, and reflect me also"
            .to_string(),
        "12- Economies fluctuate, reflect the complex interplay of global forces, and reflect me also"
            .to_string(),
        "13- Economies fluctuate, reflect the complex interplay of global forces".to_string(),
        "14- Economies fluctuate, reflect the complex interplay of global forces".to_string(),
        r"15- Strategic military campaigns alter the balance of power.".to_string(),
        r"16- Strategic military campaigns alter the balance of power.".to_string(),
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
    // row_id_bitmap.insert(5);
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
        // let retrieved_doc: Document = searcher.doc(doc_address).unwrap();

        let fast_field_reader: &Column<u64> = &fast_field_readers[doc_address.segment_ord as usize];
        let row_id = fast_field_reader
            .values_for_doc(doc_address.doc_id)
            .next()
            .unwrap();
        println!(
            "row_id:{}, score:{}, doc_id:{}, seg_id:{}, doc:{:?}",
            row_id,
            row_id_with_score.score,
            row_id_with_score.doc_id,
            row_id_with_score.seg_id,
            // schema.to_json(&retrieved_doc),
            // retrieved_doc.get_first(text).unwrap().as_text().unwrap(),
            row_id_with_score.doc
        );
    }
}
