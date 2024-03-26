#[cfg(test)]
mod tests {
    use std::cmp::min;
    use tantivy::collector::Count;
    use tantivy::query::QueryParser;
    use tempfile::TempDir;

    use crate::common::tests::{
        get_mocked_docs, index_3column_docs_with_index_writer_bridge,
        search_with_index_writer_bridge,
    };
    use crate::index::implements::api_index_impl::{
        commit_index, create_index, create_index_with_parameter, delete_row_ids, free_index_writer,
        index_multi_column_docs,
    };
    use crate::{FFI_INDEX_WRITER_CACHE, TEST_MUTEX};

    #[test]
    pub fn test_create_index_with_valid_tokenizer() {
        let _guard = TEST_MUTEX.lock().unwrap();

        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        let result = create_index_with_parameter(
            temp_directory_str,
            &vec!["col1".to_string(), "col2".to_string(), "col3".to_string()],
            "{}",
        );
        assert!(result.is_ok());

        let index_writer_bridge =
            index_3column_docs_with_index_writer_bridge(temp_directory_str, true);

        search_with_index_writer_bridge(index_writer_bridge)
    }

    #[test]
    pub fn test_create_index_with_invalid_tokenizer() {
        let _guard = TEST_MUTEX.lock().unwrap();

        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        let result = create_index_with_parameter(
            temp_directory_str,
            &vec!["col1".to_string(), "col2".to_string(), "col3".to_string()],
            "{abc}",
        );
        assert!(result.is_err());
    }

    #[test]
    pub fn test_create_index_with_not_empty_directory() {
        let _guard = TEST_MUTEX.lock().unwrap();

        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        // Create index in a clean directory.
        assert!(create_index_with_parameter(
            temp_directory_str,
            &vec!["col1".to_string(), "col2".to_string(), "col3".to_string()],
            "{}",
        )
        .is_ok());
        // Create index in a not empty directory.
        assert!(create_index_with_parameter(
            temp_directory_str,
            &vec!["col1".to_string(), "col2".to_string(), "col3".to_string()],
            "{}",
        )
        .is_ok());
    }

    #[test]
    pub fn test_create_index_by_default() {
        let _guard = TEST_MUTEX.lock().unwrap();

        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        assert!(create_index(
            temp_directory_str,
            &vec!["col1".to_string(), "col2".to_string(), "col3".to_string()]
        )
        .is_ok());
        assert!(create_index(
            temp_directory_str,
            &vec!["col1".to_string(), "col2".to_string(), "col3".to_string()]
        )
        .is_ok());
    }

    #[test]
    pub fn test_index_multi_column_docs() {
        let _guard = TEST_MUTEX.lock().unwrap();

        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();
        let column_names = vec!["col1".to_string(), "col2".to_string(), "col3".to_string()];

        assert!(create_index(temp_directory_str, &column_names).is_ok());

        // Index some documents.
        let (col1_docs, col2_docs, col3_docs) = get_mocked_docs();

        for row_id in 0..min(min(col1_docs.len(), col2_docs.len()), col3_docs.len()) {
            let column_docs = vec![
                col1_docs[row_id].clone(),
                col2_docs[row_id].clone(),
                col3_docs[row_id].clone(),
            ];
            assert!(index_multi_column_docs(
                temp_directory_str,
                row_id as u64,
                &column_names,
                &column_docs
            )
            .is_ok());
        }
        assert!(commit_index(temp_directory_str).is_ok());

        // get index writer from CACHE
        let index_writer_bridge = FFI_INDEX_WRITER_CACHE
            .get_index_writer_bridge(temp_directory_str.to_string())
            .unwrap();

        search_with_index_writer_bridge(index_writer_bridge)
    }

    #[test]
    pub fn test_index_multi_column_docs_without_writer() {
        let _guard = TEST_MUTEX.lock().unwrap();

        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();
        let column_names = vec!["col1".to_string(), "col2".to_string(), "col3".to_string()];

        // Index some documents.
        let (col1_docs, col2_docs, col3_docs) = get_mocked_docs();

        for row_id in 0..min(min(col1_docs.len(), col2_docs.len()), col3_docs.len()) {
            let column_docs = vec![
                col1_docs[row_id].clone(),
                col2_docs[row_id].clone(),
                col3_docs[row_id].clone(),
            ];
            assert!(index_multi_column_docs(
                temp_directory_str,
                row_id as u64,
                &column_names,
                &column_docs
            )
            .is_err());
        }
    }

    #[test]
    pub fn test_index_writer_commit_with_empty_directory() {
        let _guard = TEST_MUTEX.lock().unwrap();

        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        assert!(commit_index(temp_directory_str).is_err());
    }

    #[test]
    pub fn test_delete_row_ids() {
        let _guard = TEST_MUTEX.lock().unwrap();

        // Create temp ffi index writer.
        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();
        let column_names = vec!["col1".to_string(), "col2".to_string(), "col3".to_string()];

        assert!(create_index(temp_directory_str, &column_names).is_ok());

        // Index and commit some documents
        let index_writer_bridge =
            index_3column_docs_with_index_writer_bridge(temp_directory_str, false);

        // Init some necessary variables for search.
        let col1_field = index_writer_bridge
            .index
            .schema()
            .get_field("col1")
            .unwrap();
        let query_parser = QueryParser::for_index(&index_writer_bridge.index, vec![col1_field]);
        let text_query = query_parser.parse_query("Ancient").unwrap();

        // Check searched count before execute delete.
        let reader = index_writer_bridge.index.reader().unwrap();
        let count_col1 = reader.searcher().search(&text_query, &Count).unwrap();
        assert_eq!(count_col1, 2);

        assert!(delete_row_ids(temp_directory_str, &vec![0, 1, 2, 3]).is_ok());
        assert!(reader.reload().is_ok());

        // Check searched count after execute delete.
        let count_col1 = reader.searcher().search(&text_query, &Count).unwrap();
        assert_eq!(count_col1, 1);
    }

    #[test]
    pub fn test_free_index_writer() {
        let _guard = TEST_MUTEX.lock().unwrap();

        // Create temp ffi index writer.
        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();
        let column_names = vec!["col1".to_string(), "col2".to_string(), "col3".to_string()];

        let index_writer_bridge_init =
            create_index_with_parameter(temp_directory_str, &column_names, "{}");
        assert!(index_writer_bridge_init.is_ok());

        // Get index writer from CACHE
        let index_writer_bridge = FFI_INDEX_WRITER_CACHE
            .get_index_writer_bridge(temp_directory_str.to_string())
            .unwrap();
        assert!(index_writer_bridge.commit().is_ok());

        // Test whether index_writer is exist after `tantivy_writer_free`.
        assert!(index_writer_bridge
            .writer
            .try_lock()
            .unwrap()
            .as_mut()
            .is_some());

        assert!(free_index_writer(temp_directory_str).is_ok());

        assert!(index_writer_bridge
            .writer
            .try_lock()
            .unwrap()
            .as_mut()
            .is_none());
    }
}
