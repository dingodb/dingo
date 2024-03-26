#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::common::tests::index_3column_docs_with_threads_merge;
    use crate::search::implements::api_common_impl::{
        free_index_reader, get_indexed_doc_counts, load_index_reader,
    };
    use crate::FFI_INDEX_SEARCHER_CACHE;

    #[test]
    pub fn test_load_index_reader() {
        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        index_3column_docs_with_threads_merge(temp_directory_str);

        assert!(FFI_INDEX_SEARCHER_CACHE
            .get_index_reader_bridge(temp_directory_str.to_string())
            .is_err());

        assert!(load_index_reader(temp_directory_str).is_ok());

        assert!(FFI_INDEX_SEARCHER_CACHE
            .get_index_reader_bridge(temp_directory_str.to_string())
            .is_ok());
    }

    #[test]
    pub fn test_free_index_reader() {
        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        index_3column_docs_with_threads_merge(temp_directory_str);
        assert!(load_index_reader(temp_directory_str).is_ok());

        assert!(FFI_INDEX_SEARCHER_CACHE
            .get_index_reader_bridge(temp_directory_str.to_string())
            .is_ok());

        assert!(free_index_reader(temp_directory_str).is_ok());

        assert!(FFI_INDEX_SEARCHER_CACHE
            .get_index_reader_bridge(temp_directory_str.to_string())
            .is_err());
    }

    #[test]
    pub fn test_get_indexed_doc_counts() {
        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        index_3column_docs_with_threads_merge(temp_directory_str);

        assert!(load_index_reader(temp_directory_str).is_ok());

        let res = get_indexed_doc_counts(temp_directory_str);

        assert!(res.is_ok());
        assert_eq!(res.clone().unwrap(), 5);
    }
}
