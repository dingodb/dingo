#[cfg(test)]
mod tests{
    use tantivy::{Document, Index, IndexReader};
    use cxx::{let_cxx_string, CxxString, CxxVector};
    use tantivy::query::QueryParser;
    use tempfile::TempDir;

    use crate::{search::collector::row_id_bitmap_collector::RowIdRoaringCollector, tantivy_create_index, tantivy_create_index_with_tokenizer, tantivy_delete_row_ids, tantivy_index_doc, tantivy_writer_commit, tantivy_writer_free, FFI_INDEX_WRITER_CACHE};

    fn commit_some_docs_for_test(index_directory: String, waiting_merging_threads_finished: bool) -> (QueryParser, Index) {
        // get index writer from CACHE
        let index_writer_bridge = FFI_INDEX_WRITER_CACHE.get_index_writer_bridge(index_directory).unwrap();
        
        // Get fields from `schema`.
        let row_id_field = index_writer_bridge.index.schema().get_field("row_id").expect("Can't get row_id filed");
        let text_field = index_writer_bridge.index.schema().get_field("text").expect("Can't get text filed");

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
            let result = index_writer_bridge.add_document(doc);
            assert!(result.is_ok());
        }
        assert!(index_writer_bridge.commit().is_ok());
        if waiting_merging_threads_finished {
            assert!(index_writer_bridge.wait_merging_threads().is_ok());   
        }
        (QueryParser::for_index(&index_writer_bridge.index, vec![text_field]), index_writer_bridge.index.clone())
    }

    #[test]
    pub fn test_create_index_with_valid_tokenizer(){
        let temp_directory = TempDir::new().expect("Can't create temp directory");
        let temp_directory_str = temp_directory.path().to_str().expect("Can't convert temp directory to temp directory_str");
        let_cxx_string!(tokenizer_with_parameter = "whitespace(false)");
        let_cxx_string!(index_directory = temp_directory_str);
        let tokenizer_with_parameter_cxx: &CxxString = tokenizer_with_parameter.as_ref().get_ref();
        let index_directory_cxx: &CxxString = index_directory.as_ref().get_ref();
        let result = tantivy_create_index_with_tokenizer(index_directory_cxx, tokenizer_with_parameter_cxx, false);
        assert!(result.is_ok());

        // Init some necessary variables for search.
        let (query_parser, index) = commit_some_docs_for_test(temp_directory_str.to_string(), true);
        let text_query = query_parser.parse_query("Ancient").expect("Can't parse query");
        let row_id_collector = RowIdRoaringCollector::with_field("row_id".to_string());

        // Test whether index can be use.
        let searched_bitmap_1 = index.reader().unwrap().searcher().search(&text_query, &row_id_collector).expect("Can't execute search.");
        assert_eq!(searched_bitmap_1.len(), 2);
    }

    #[test]
    pub fn test_create_index_with_invalid_tokenizer(){
        let temp_directory = TempDir::new().expect("Can't create temp directory");
        let_cxx_string!(tokenizer_with_parameter = "default(ABC)");
        let_cxx_string!(index_directory = temp_directory.path().to_str().expect("Can't convert temp directory to temp directory_str"));
        let tokenizer_with_parameter_cxx: &CxxString = tokenizer_with_parameter.as_ref().get_ref();
        let index_directory_cxx: &CxxString = index_directory.as_ref().get_ref();
        let result = tantivy_create_index_with_tokenizer(index_directory_cxx, tokenizer_with_parameter_cxx, false);
        assert!(result.is_err());
    }

    #[test]
    pub fn test_create_index_with_not_empty_directory(){
        let temp_directory = TempDir::new().expect("Can't create temp directory");
        let temp_directory_str = temp_directory.path().to_str().unwrap();
        // Create some cxx parameters.
        let_cxx_string!(tokenizer_with_parameter = "default");
        let_cxx_string!(index_directory = temp_directory_str);
        let tokenizer_with_parameter_cxx: &CxxString = tokenizer_with_parameter.as_ref().get_ref();
        let index_directory_cxx: &CxxString = index_directory.as_ref().get_ref();
        // Create index in a clean directory.
        assert!(tantivy_create_index_with_tokenizer(index_directory_cxx, tokenizer_with_parameter_cxx, false).is_ok());
        // Create index in a not empty directory.
        assert!(tantivy_create_index_with_tokenizer(index_directory_cxx, tokenizer_with_parameter_cxx, false).is_ok());
    }

    #[test]
    pub fn test_create_index_by_default(){
        let temp_directory = TempDir::new().expect("Can't create temp directory");
        let temp_directory_str = temp_directory.path().to_str().expect("Can't convert temp directory to temp directory_str");
        let_cxx_string!(index_directory = temp_directory_str);
        let index_directory_cxx: &CxxString = index_directory.as_ref().get_ref();
        assert!(tantivy_create_index(index_directory_cxx, false).is_ok());
        assert!(tantivy_create_index(index_directory_cxx, true).is_ok());

    }

    #[test]
    pub fn test_index_and_commit_doc(){
        let temp_directory = TempDir::new().expect("Can't create temp directory");
        let temp_directory_str = temp_directory.path().to_str().expect("Can't convert temp directory to temp directory_str");
        let_cxx_string!(index_directory = temp_directory_str);
        let index_directory_cxx: &CxxString = index_directory.as_ref().get_ref();
        assert!(tantivy_create_index(index_directory_cxx, true).is_ok());

        // Index some documents.
        let docs: Vec<String> = vec![
            "Ancient empires rise and fall, shaping history's course.".to_string(),
            "Artistic expressions reflect diverse cultural heritages.".to_string(),
            "Social movements transform societies, forging new paths.".to_string(),
            "Strategic military campaigns alter the balance of power.".to_string(),
            "Ancient philosophies provide wisdom for modern dilemmas.".to_string()
        ];
        for row_id in 0..docs.len() {
            let_cxx_string!(doc = &docs[row_id]);
            let doc_cxx = doc.as_ref().get_ref();
            assert!(tantivy_index_doc(index_directory_cxx, row_id as u64, doc_cxx).is_ok());
        }
        assert!(tantivy_writer_commit(index_directory_cxx).is_ok());

        // get index writer from CACHE
        let index_writer_bridge = FFI_INDEX_WRITER_CACHE.get_index_writer_bridge(temp_directory_str.to_string()).unwrap();

        // Get fields from `schema`.
        let text_field = index_writer_bridge.index.schema().get_field("text").expect("Can't get text filed");

        // Test whether index can be use.
        let query_parser = QueryParser::for_index(&index_writer_bridge.index, vec![text_field]);
        let searcher =index_writer_bridge.index.reader().unwrap().searcher();
        let row_id_collector = RowIdRoaringCollector::with_field("row_id".to_string());
        let text_query = query_parser.parse_query("Ancient").expect("Can't parse query");
        let searched_bitmap_1 = searcher.search(&text_query, &row_id_collector).expect("Can't execute search.");
        assert_eq!(searched_bitmap_1.len(), 2);
    }

    #[test]
    pub fn test_index_doc_without_index_writer(){
        let temp_directory = TempDir::new().expect("Can't create temp directory");
        let temp_directory_str = temp_directory.path().to_str().expect("Can't convert temp directory to temp directory_str");
        let_cxx_string!(index_directory = temp_directory_str);
        let index_directory_cxx: &CxxString = index_directory.as_ref().get_ref();
        let_cxx_string!(doc = "Ancient philosophies provide wisdom for modern dilemmas.");
        let doc_cxx = doc.as_ref().get_ref();
        assert!(tantivy_index_doc(index_directory_cxx, 0, doc_cxx).is_err());
    }

    #[test]
    pub fn test_index_writer_commit_with_empty_directory(){
        let temp_directory = TempDir::new().expect("Can't create temp directory");
        let temp_directory_str = temp_directory.path().to_str().expect("Can't convert temp directory to temp directory_str");
        let_cxx_string!(index_directory = temp_directory_str);
        let index_directory_cxx: &CxxString = index_directory.as_ref().get_ref();
        assert!(tantivy_writer_commit(index_directory_cxx).is_err());
    }

    #[test]
    pub fn test_delete_row_ids(){
        // Create temp ffi index writer.
        let temp_directory = TempDir::new().expect("Can't create temp directory");
        let temp_directory_str = temp_directory.path().to_str().expect("Can't convert temp directory to temp directory_str");
        let_cxx_string!(tokenizer_with_parameter = "whitespace(false)");
        let_cxx_string!(index_directory = temp_directory_str);
        let tokenizer_with_parameter_cxx: &CxxString = tokenizer_with_parameter.as_ref().get_ref();
        let index_directory_cxx: &CxxString = index_directory.as_ref().get_ref();
        let result = tantivy_create_index_with_tokenizer(index_directory_cxx, tokenizer_with_parameter_cxx, false);
        assert!(result.is_ok());

        // Index and commit some documents
        let (query_parser, index) = commit_some_docs_for_test(temp_directory_str.to_string(), false);

        // Init some necessary variables for search.
        let text_query = query_parser.parse_query("Ancient").expect("Can't parse query");
        let row_id_collector = RowIdRoaringCollector::with_field("row_id".to_string());

        // Check searched count before execute delete.
        let searched_bitmap_1 = index.reader().unwrap().searcher().search(&text_query, &row_id_collector).expect("Can't execute search.");
        assert_eq!(searched_bitmap_1.len(), 2);

        // Create a variable named `vector_cxx`, it stores rowids need to be deleted.
        let mut vector_cxx: cxx::UniquePtr<CxxVector<u32>> = CxxVector::new();
        let vector_cxx_mut = vector_cxx.as_mut();
        if let Some(mut pinned_vector) = vector_cxx_mut {
            for row_id in 0..4 {
                pinned_vector.as_mut().push(row_id);
            }
            pinned_vector.as_mut().push(1000);
            pinned_vector.as_mut().push(999999);
        }
        // Delete a group of terms
        assert!(tantivy_delete_row_ids(index_directory_cxx, &vector_cxx).is_ok());
        // assert!(index.reader().unwrap().reload().is_ok());

        // Check searched count after execute delete.
        let searched_bitmap_2 = index.reader().unwrap().searcher().search(&text_query, &row_id_collector).expect("Can't execute search.");
        assert_eq!(searched_bitmap_2.len(), 1);
    }

    #[test]
    pub fn test_free_index_writer(){
        // Create temp ffi index writer.
        let temp_directory = TempDir::new().expect("Can't create temp directory");
        let temp_directory_str = temp_directory.path().to_str().expect("Can't convert temp directory to temp directory_str");
        let_cxx_string!(tokenizer_with_parameter = "whitespace(false)");
        let_cxx_string!(index_directory = temp_directory_str);
        let tokenizer_with_parameter_cxx: &CxxString = tokenizer_with_parameter.as_ref().get_ref();
        let index_directory_cxx: &CxxString = index_directory.as_ref().get_ref();
        let index_writer_bridge_init = tantivy_create_index_with_tokenizer(index_directory_cxx, tokenizer_with_parameter_cxx, false);
        assert!(index_writer_bridge_init.is_ok());

        // Get index writer from CACHE
        let index_writer_bridge = FFI_INDEX_WRITER_CACHE.get_index_writer_bridge(temp_directory_str.to_string()).unwrap();
        assert!(index_writer_bridge.commit().is_ok());

        // Test whether index_writer is exist after `tantivy_writer_free`.
        assert!(index_writer_bridge.writer.try_lock().unwrap().as_mut().is_some());
        assert!(tantivy_writer_free(index_directory_cxx).is_ok());
        assert!(index_writer_bridge.writer.try_lock().unwrap().as_mut().is_none());
    }
}