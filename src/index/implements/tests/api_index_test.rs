#[cfg(test)]
mod tests {
    use std::cmp::min;
    use std::sync::Arc;
    use tantivy::collector::Count;
    use tantivy::query::QueryParser;
    use tantivy::Document;
    use tempfile::TempDir;

    use crate::index::bridge::index_writer_bridge::IndexWriterBridge;
    use crate::index::implements::api_index_impl::{
        commit_index, create_index, create_index_with_parameter, delete_row_ids, free_index_writer,
        index_multi_column_docs,
    };
    use crate::{FFI_INDEX_WRITER_CACHE, TEST_MUTEX};

    fn get_mocked_docs() -> (Vec<String>, Vec<String>, Vec<String>) {
        let col1_docs: Vec<String> = vec![
            "Ancient empires rise and fall, shaping history's course.".to_string(),
            "Artistic expressions reflect diverse cultural heritages.".to_string(),
            "Social movements transform societies, forging new paths.".to_string(),
            "Strategic military campaigns alter the balance of power.".to_string(),
            "Ancient philosophies provide wisdom for modern dilemmas.".to_string(),
        ];
        let col2_docs: Vec<String> = vec![
            "Brave explorers venture into uncharted territories, expanding horizons.".to_string(),
            "Brilliant minds unravel nature's mysteries through scientific inquiry.".to_string(),
            "Economic systems evolve, influencing global trade and prosperity.".to_string(),
            "Environmental challenges demand innovative solutions for sustainability.".to_string(),
            "Ethical dilemmas test the boundaries of moral reasoning and judgment.".to_string(),
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

    fn do_some_mocked_search(index_writer_bridge: Arc<IndexWriterBridge>) {
        // Get fields from `schema`.
        let col1_field = index_writer_bridge
            .index
            .schema()
            .get_field("col1")
            .expect("Can't get col1 filed");
        let col2_field = index_writer_bridge
            .index
            .schema()
            .get_field("col2")
            .expect("Can't get col2 filed");
        let col3_field = index_writer_bridge
            .index
            .schema()
            .get_field("col3")
            .expect("Can't get col3 filed");
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

    fn index_three_column_docs(
        index_directory: String,
        waiting_merging_threads_finished: bool,
    ) -> Arc<IndexWriterBridge> {
        // Get index writer from CACHE
        let index_writer_bridge = FFI_INDEX_WRITER_CACHE
            .get_index_writer_bridge(index_directory)
            .unwrap();

        // Get fields from `schema`.
        let row_id_field = index_writer_bridge
            .index
            .schema()
            .get_field("row_id")
            .expect("Can't get row_id filed");
        let col1_field = index_writer_bridge
            .index
            .schema()
            .get_field("col1")
            .expect("Can't get col1 filed");
        let col2_field = index_writer_bridge
            .index
            .schema()
            .get_field("col2")
            .expect("Can't get col2 filed");
        let col3_field = index_writer_bridge
            .index
            .schema()
            .get_field("col3")
            .expect("Can't get col3 filed");

        // Index some documents.
        let (col1_docs, col2_docs, col3_docs) = get_mocked_docs();

        for row_id in 0..5 {
            let mut doc = Document::default();
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

        let index_writer_bridge = index_three_column_docs(temp_directory_str.to_string(), true);

        do_some_mocked_search(index_writer_bridge)
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

        do_some_mocked_search(index_writer_bridge)
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
        let index_writer_bridge = index_three_column_docs(temp_directory_str.to_string(), false);

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
