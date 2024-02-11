use std::sync::Mutex;
use crate::logger::ffi_logger::callback_with_thread_info;
use crate::{common::constants::LOG_CALLBACK, INFO};
use tantivy::{Document, Index, IndexWriter, Opstamp, Term};

pub struct FFiIndexWriter {
    pub path: String,
    pub index: Index,
    pub writer: Mutex<Option<IndexWriter>>,
}

impl FFiIndexWriter {
    // wrapper for IndexWriter.commit()
    pub fn commit(&self) -> Result<Opstamp, String> {
        match self.writer.lock() {
            Ok(mut writer) => {
                if let Some(writer) = writer.as_mut() {
                    writer.commit().map_err(|e| e.to_string())
                    // let commit_status = writer.commit();
                    // self.index.reader().unwrap().reload().map_err(|e| e.to_string());
                    // commit_status.map_err(|e| e.to_string())
                } else {
                    Err("IndexWriter is not available".to_string())
                }
            }
            Err(e) => Err(format!("Lock error: {}", e)),
        }
    }

    // wrapper for IndexWriter.add_document()
    pub fn add_document(&self, document: Document) -> Result<Opstamp, String> {
        match self.writer.lock() {
            Ok(mut writer) => {
                if let Some(writer) = writer.as_mut() {
                    writer.add_document(document).map_err(|e| e.to_string())
                } else {
                    Err("IndexWriter is not available".to_string())
                }
            }
            Err(e) => Err(format!("Lock error: {}", e)),
        }
    }

    // wrapper for IndexWriter.delete_term()
    #[allow(dead_code)]
    pub fn delete_term(&self, term: Term) -> Result<Opstamp, String> {
        match self.writer.lock() {
            Ok(mut writer) => {
                if let Some(writer) = writer.as_mut() {
                    Ok(writer.delete_term(term))
                } else {
                    Err("IndexWriter is not available for delete_term".to_string())
                }
            }
            Err(e) => Err(format!("Lock error: {}", e)),
        }
    }

    // Delete a group of terms.
    pub fn delete_terms(&self, terms: Vec<Term>) -> Result<Opstamp, String> {
        match self.writer.lock() {
            Ok(mut writer) => {
                if let Some(writer) = writer.as_mut() {
                    let mut opstamp: Opstamp = 0;
                    for term in terms {
                        opstamp = writer.delete_term(term)
                    }
                    Ok(opstamp)
                } else {
                    Err("IndexWriter is not available for delete_term".to_string())
                }
            }
            Err(e) => Err(format!("Lock error: {}", e)),
        }
    }

    // Wrapper for IndexWriter.wait_merging_threads().
    pub fn wait_merging_threads(&self) -> Result<(), String> {
        // use Interior Mutability
        match self.writer.lock() {
            Ok(mut writer) => {
                if let Some(writer) = writer.take() {
                    let _ = writer.wait_merging_threads();
                };
                Ok(())
            }
            Err(e) => Err(format!("Failed to acquire lock in drop: {}", e.to_string())),
        }
    }
}

impl Drop for FFiIndexWriter {
    fn drop(&mut self) {
        INFO!("IndexW has been dropped. index_path:[{}]", self.path);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use tantivy::{merge_policy::LogMergePolicy, query::QueryParser, schema::{Schema, FAST, INDEXED, STORED, TEXT}, Document, Index, Term};
    use tempfile::TempDir;
    use crate::search::row_id_bitmap_collector::RowIdRoaringCollector;
    use crate::index::ffi_index_writer::FFiIndexWriter;

    fn create_index_in_temp_directory(index_directory_str: &str) -> FFiIndexWriter {
        // Construct the schema for the index.
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("row_id", FAST | INDEXED);
        schema_builder.add_text_field("text", TEXT | STORED);
        let schema = schema_builder.build();
        // Create the index in the specified directory.
        let index = Index::create_in_dir(index_directory_str.to_string(), schema).expect("Can't create index");
        // Create the writer with a specified buffer size (e.g., 64 MB).
        let writer = index.writer_with_num_threads(2, 1024 * 1024 * 64).expect("Can't create index writer");
        // Configure default merge policy
        writer.set_merge_policy(Box::new(LogMergePolicy::default()));
        // Generate ffiIndexWriter.
        let ffi_index_writer = FFiIndexWriter {
            index,
            path: index_directory_str.to_string(), 
            writer: Mutex::new(Some(writer)),
        };
        return ffi_index_writer;
    }

    fn index_some_docs_for_test(ffi_index_writer: &FFiIndexWriter) -> QueryParser {
        // Get fields from `schema`.
        let row_id_field = ffi_index_writer.index.schema().get_field("row_id").expect("Can't get row_id filed");
        let text_field = ffi_index_writer.index.schema().get_field("text").expect("Can't get text filed");

        // Index some documents.
        let docs: Vec<String> = vec![
            "Ancient empires rise and fall, shaping history's course.".to_string(),
            "Artistic expressions reflect diverse cultural heritages.".to_string(),
            "Social movements transform societies, forging new paths.".to_string(),
            "Strategic military campaigns alter the balance of power.".to_string(),
            "Ancient philosophies provide wisdom for modern dilemmas.".to_string()
        ];
        for row_id in 0..docs.len() {
            let mut doc = Document::default();
            doc.add_u64(row_id_field, row_id as u64);
            doc.add_text(text_field, &docs[row_id]);
            let result = ffi_index_writer.add_document(doc);
            assert!(result.is_ok());
        }
        QueryParser::for_index(&ffi_index_writer.index, vec![text_field])
    }

    #[test]
    pub fn test_add_document_and_commit(){
        // Create a temp directory for test.
        let directory = TempDir::new().expect("Can't create temp directory");
        let directory_str = directory.path().to_str().unwrap();

        // Initialize a temp `FFiIndexWriter` for test.
        let ffi_index_writer = create_index_in_temp_directory(directory_str);

        // Prepare some docs for search.
        let query_parser = index_some_docs_for_test(&ffi_index_writer);
        
        // Init some necessary variables for search.
        let text_query = query_parser.parse_query("Ancient").expect("Can't parse query");
        let row_id_collector = RowIdRoaringCollector::with_field("row_id".to_string());


        // Execute a query before commit.
        let searcher_1 = ffi_index_writer.index.reader().expect("Can't get reader from index").searcher();
        let searched_bitmap_1 = searcher_1.search(&text_query, &row_id_collector).expect("Can't execute search.");
        assert_eq!(searched_bitmap_1.len(), 0);

        // Execute `FFiIndexWriter` commit.
        assert!(ffi_index_writer.commit().is_ok());

        // Execute a qeury after commit.
        let searcher_2 = ffi_index_writer.index.reader().expect("Can't get reader from index").searcher();
        let searched_bitmap_2 = searcher_2.search(&text_query, &row_id_collector).expect("Can't execute search.");
        assert_eq!(searched_bitmap_2.len(), 2);

    }

    #[test]
    pub fn test_wait_merging_threads() {
        // Create a temp directory for test.
        let directory = TempDir::new().expect("Can't create temp directory");
        let directory_str = directory.path().to_str().unwrap();

        // Initialize a temp `FFiIndexWriter` for test.
        let ffi_index_writer = create_index_in_temp_directory(directory_str);

        // Prepare some docs for search.
        let _ = index_some_docs_for_test(&ffi_index_writer);
        // Waitting an exist FFiIndexWriter merging threads finished.
        let merge_status_a = ffi_index_writer.wait_merging_threads();
        assert!(merge_status_a.is_ok());

        {
            // `IndexWriter` inner `FFiIndexWriter` should be none after merging threads.
            let lock = ffi_index_writer.writer.lock().unwrap();
            let index_writer_some = &*lock;
            assert!(index_writer_some.is_none());
        }
        
        // Waitting a not exist FFiIndexWriter merging threads is fine.
        let merge_status_b = ffi_index_writer.wait_merging_threads();
        assert!(merge_status_b.is_ok());
    }

    #[test]
    pub fn test_delete_terms() {
        // Create a temp directory for test.
        let directory = TempDir::new().expect("Can't create temp directory");
        let directory_str = directory.path().to_str().unwrap();

        // Initialize a temp `FFiIndexWriter` for test.
        let ffi_index_writer = create_index_in_temp_directory(directory_str);
        let row_id_field: tantivy::schema::Field = ffi_index_writer.index.schema().get_field("row_id").expect("Can't get `row_id` field.");

        // Prepare some docs for search.
        let query_parser = index_some_docs_for_test(&ffi_index_writer);
        assert!(ffi_index_writer.commit().is_ok());

        // Init some necessary variables for search.
        let text_query = query_parser.parse_query("Ancient").expect("Can't parse query");
        let row_id_collector = RowIdRoaringCollector::with_field("row_id".to_string());

        // Execute a query before delete a group of terms.
        let searcher_1 = ffi_index_writer.index.reader().expect("Can't get reader from index").searcher();
        let searched_bitmap_1 = searcher_1.search(&text_query, &row_id_collector).expect("Can't execute search.");
        assert_eq!(searched_bitmap_1.len(), 2);

        // Delete a group of terms.
        let row_ids = vec![0, 1, 2];
        let terms: Vec<Term> = row_ids
            .iter()
            .map(|row_id| Term::from_field_u64(
                row_id_field,
                *row_id as u64
            ))
            .collect();
        assert!(ffi_index_writer.delete_terms(terms).is_ok());
        assert!(ffi_index_writer.commit().is_ok());

        // Execute a query after delete a group of terms.
        let searcher_2 = ffi_index_writer.index.reader().expect("Can't get reader from index").searcher();
        let searched_bitmap_2 = searcher_2.search(&text_query, &row_id_collector).expect("Can't execute search.");
        assert_eq!(searched_bitmap_2.len(), 1);

        // Delete a specific term.
        let term = Term::from_field_u64(row_id_field, 4);
        assert!(ffi_index_writer.delete_term(term).is_ok());
        assert!(ffi_index_writer.commit().is_ok());
       
        // Execute a query after delete the specific term.
        let searcher_3 = ffi_index_writer.index.reader().expect("Can't get reader from index").searcher();
        let searched_bitmap_3 = searcher_3.search(&text_query, &row_id_collector).expect("Can't execute search.");
        assert_eq!(searched_bitmap_3.len(), 0);
    }
}
