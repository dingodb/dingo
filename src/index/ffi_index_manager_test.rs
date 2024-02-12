#[cfg(test)]
mod tests{
    use tantivy::{Document, Searcher};
    use cxx::{let_cxx_string, CxxString};
    use tantivy::query::QueryParser;
    use tempfile::TempDir;

    use crate::{search::row_id_bitmap_collector::RowIdRoaringCollector, tantivy_create_index_with_tokenizer, FFI_INDEX_WRITER_CACHE};

    fn commit_some_docs_for_test(index_directory: String) -> (QueryParser, Searcher) {
        // get index writer from CACHE
        let ffi_index_writer = FFI_INDEX_WRITER_CACHE.get_ffi_index_writer(index_directory).unwrap();
        
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
        assert!(ffi_index_writer.commit().is_ok());
        assert!(ffi_index_writer.wait_merging_threads().is_ok());
        (QueryParser::for_index(&ffi_index_writer.index, vec![text_field]), ffi_index_writer.index.reader().unwrap().searcher())
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
        let (query_parser, searcher) = commit_some_docs_for_test(temp_directory_str.to_string());
        let text_query = query_parser.parse_query("Ancient").expect("Can't parse query");
        let row_id_collector = RowIdRoaringCollector::with_field("row_id".to_string());

        // Test whether index can be use.
        let searched_bitmap_1 = searcher.search(&text_query, &row_id_collector).expect("Can't execute search.");
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
}