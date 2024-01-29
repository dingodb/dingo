use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use roaring::RoaringBitmap;
use tantivy::collector::TopDocs;
use tantivy::fastfield::Column;
use tantivy::query::{Query, QueryParser, TermQuery};
use tantivy::schema::{Field, IndexRecordOption, Schema, FAST, INDEXED, STORED, TEXT};
use tantivy::{DocAddress, Document, Index, IndexReader, Searcher, SegmentReader, Term};
use tantivy_search::ffi::RowIdWithScore;
use tantivy_search::search::top_dos_with_bitmap_collector::TopDocsWithFilter;

#[allow(dead_code)]
fn get_doc_with_given_term(
    reader: &IndexReader,
    given_term: &Term,
) -> tantivy::Result<Option<Document>> {
    let searcher = reader.searcher();

    // This is the simplest query you can think of.
    // It matches all of the documents containing a specific term.
    //
    // The second argument is here to tell we don't care about decoding positions,
    // or term frequencies.
    let term_query = TermQuery::new(given_term.clone(), IndexRecordOption::Basic);
    let top_docs = searcher.search(&term_query, &TopDocs::with_limit(1))?;

    if let Some((_score, doc_address)) = top_docs.first() {
        let doc = searcher.doc(*doc_address)?;
        Ok(Some(doc))
    } else {
        // no doc matching this ID.
        Ok(None)
    }
}

// &Box generates a second-level pointer dereference
fn print_search_results(searcher: &Searcher, text_field: &Field, query: &Box<dyn Query>) {
    // ffrs for row_id.
    let fast_field_readers: Vec<Column<u64>> = searcher
        .segment_readers()
        .iter()
        .map(|segment_reader: &SegmentReader| {
            let fast_fields: &tantivy::fastfield::FastFieldReaders = segment_reader.fast_fields();
            fast_fields.u64("row_id").unwrap()
        })
        .collect();
    // collector for searcher.
    let mut row_id_bitmap = RoaringBitmap::new();
    row_id_bitmap.extend(0..10);
    let collector = TopDocsWithFilter::with_limit(10)
        .with_alive(Arc::new(row_id_bitmap))
        .with_searcher(searcher.clone()) // auto use reload searcher().
        .with_text_field(*text_field)
        .with_stored_text(true);

    let top_docs: Vec<RowIdWithScore> = searcher.search(query, &collector).unwrap();
    for row_id_with_score in top_docs {
        // assert row_id equals.
        let doc_address = DocAddress {
            segment_ord: row_id_with_score.seg_id,
            doc_id: row_id_with_score.doc_id,
        };
        let fast_field_reader: &Column<u64> = &fast_field_readers[doc_address.segment_ord as usize];
        assert_eq!(
            fast_field_reader
                .values_for_doc(doc_address.doc_id)
                .next()
                .unwrap(),
            row_id_with_score.row_id
        );

        println!(
            "row_id:{}, score:{}, doc_id:{}, seg_id:{}, doc:{:?}",
            row_id_with_score.row_id,
            row_id_with_score.score,
            row_id_with_score.doc_id,
            row_id_with_score.seg_id,
            row_id_with_score.doc
        );
    }
}

fn main() {
    let mut schema_builder = Schema::builder();
    let row_id = schema_builder.add_u64_field("row_id", FAST | INDEXED | STORED);
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let text = schema_builder.add_text_field("text", TEXT | STORED);
    let schema = schema_builder.build();
    // let index = Index::create_in_ram(schema.clone());
    let index = Index::create_in_dir("./temp_index", schema.clone()).unwrap();

    let mut index_writer = index
        .writer_with_num_threads(2, 1024 * 1024 * 1024)
        .unwrap();

    let str_vec: Vec<(String, String)> = vec![
        ("frank's house".to_string(), "0- Ancient empires shape history's course".to_string()),
        ("Jerry".to_string(), "1- Ancient empires shape history's course".to_string()),
        ("Tom".to_string(), "2- New ideas rise, inspiring innovation.".to_string()),
        ("Frank".to_string(), "3- New ideas rise, inspiring innovation.".to_string()),
        ("Not bad".to_string(), "4- Ancient empires rise and fall, shaping history's course.".to_string()),
        ("White House".to_string(), "5- Ancient empires rise and fall, shaping history's course.".to_string()),
        ("embedding".to_string(), "6- Artistic expressions reflect diverse cultural heritages.".to_string()),
        ("guess what".to_string(), "7- Artistic expressions reflect diverse cultural heritages.".to_string()),
        ("ahaha".to_string(), "8- Social movements transform societies, forging new paths.".to_string()),
        ("you and me".to_string(), "9- Social movements transform societies, forging new paths.".to_string()),
        ("just happy".to_string(), "10- Economies fluctuate, reflect the complex interplay of global forces, and reflect me also".to_string()),
        ("music too...".to_string(), "11- Economies fluctuate, reflect the complex interplay of global forces, and reflect me also".to_string()),
        ("oh no".to_string(), "12- Economies fluctuate, reflect the complex interplay of global forces".to_string()),
        ("hello".to_string(), "13- Economies fluctuate, reflect the complex interplay of global forces".to_string()),
        ("test title".to_string(), r"14- Strategic military campaigns alter the balance of power.".to_string()),
        ("test house".to_string(), r"15- Strategic military campaigns alter the balance of power.".to_string()),
    ];

    for i in 0..str_vec.len() {
        let mut temp = Document::default();
        temp.add_u64(row_id, i as u64);
        temp.add_text(title, &str_vec[i].0);
        temp.add_text(text, &str_vec[i].1);
        let _ = index_writer.add_document(temp);
    }
    index_writer.commit().unwrap();
    let reader = index.reader().unwrap();

    // query for fuzzy search.
    let query_parser = QueryParser::for_index(&index, vec![text]);
    let query: Box<dyn Query> = query_parser.parse_query("reflect").unwrap();

    println!("=================== before delete row_id 10 ===================");
    print_search_results(&reader.searcher(), &text, &query);
    sleep(Duration::from_secs(5));

    println!("==================== after delete row_id 10 ===================");
    let term_rowid_10 = Term::from_field_u64(row_id, 10);
    index_writer.delete_term(term_rowid_10.clone());
    index_writer.commit().unwrap();
    let _ = reader.reload();
    print_search_results(&reader.searcher(), &text, &query);
    sleep(Duration::from_secs(5));

    println!("==================== after delete term `also` ===================");
    let term_doc_also = Term::from_field_text(text, "also");
    index_writer.delete_term(term_doc_also.clone());
    index_writer.commit().unwrap();
    let _ = reader.reload();
    print_search_results(&reader.searcher(), &text, &query);

    println!("==================== after delete row_id 13 ===================");
    let term_rowid_13 = Term::from_field_u64(row_id, 13);
    index_writer.delete_term(term_rowid_13.clone());
    index_writer.commit().unwrap();
    let _ = reader.reload();
    print_search_results(&reader.searcher(), &text, &query);
    sleep(Duration::from_secs(5));

    println!("==================== after delete row_id 12 ===================");
    let term_rowid_12 = Term::from_field_u64(row_id, 12);
    index_writer.delete_term(term_rowid_12.clone());
    index_writer.commit().unwrap();
    let _ = reader.reload();
    print_search_results(&reader.searcher(), &text, &query);
    sleep(Duration::from_secs(5));

    println!("==================== after reinsert ===================");
    for i in 0..str_vec.len() {
        let mut temp = Document::default();
        temp.add_u64(row_id, i as u64);
        temp.add_text(title, &str_vec[i].0);
        temp.add_text(text, &str_vec[i].1);
        let _ = index_writer.add_document(temp);
    }
    index_writer.commit().unwrap();
    let _ = reader.reload();
    print_search_results(&reader.searcher(), &text, &query);
    sleep(Duration::from_secs(5));
}
