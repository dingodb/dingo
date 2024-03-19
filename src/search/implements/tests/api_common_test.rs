#[cfg(test)]
mod tests {
    use tantivy::merge_policy::LogMergePolicy;
    use tantivy::schema::{Schema, FAST, INDEXED, TEXT};
    use tantivy::{Document, Index};
    use tempfile::TempDir;

    use crate::search::implements::api_common_impl::{
        free_index_reader, get_indexed_doc_counts, load_index_reader,
    };
    use crate::{FFI_INDEX_SEARCHER_CACHE, TEST_MUTEX};

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

    fn index_three_column_docs(index_directory: &str) {
        // Construct the schema for the index.
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("row_id", FAST | INDEXED);
        schema_builder.add_text_field("col1", TEXT);
        schema_builder.add_text_field("col2", TEXT);
        schema_builder.add_text_field("col3", TEXT);
        let schema = schema_builder.build();

        // Create the index in the specified directory.
        let index = Index::create_in_dir(index_directory, schema.clone()).unwrap();
        // Create the writer with a specified buffer size (e.g., 64 MB).
        let mut writer = index.writer_with_num_threads(2, 1024 * 1024 * 64).unwrap(); // Configure default merge policy.
        writer.set_merge_policy(Box::new(LogMergePolicy::default()));

        // Get fields from `schema`.
        let row_id_field = index.schema().get_field("row_id").unwrap();
        let col1_field = index.schema().get_field("col1").unwrap();
        let col2_field = index.schema().get_field("col2").unwrap();
        let col3_field = index.schema().get_field("col3").unwrap();

        // Index some documents.
        let (col1_docs, col2_docs, col3_docs) = get_mocked_docs();

        for row_id in 0..5 {
            let mut doc = Document::default();
            doc.add_u64(row_id_field, row_id as u64);
            doc.add_text(col1_field, &col1_docs[row_id]);
            doc.add_text(col2_field, &col2_docs[row_id]);
            doc.add_text(col3_field, &col3_docs[row_id]);
            let result = writer.add_document(doc);
            assert!(result.is_ok());
        }
        assert!(writer.commit().is_ok());
        assert!(writer.wait_merging_threads().is_ok())
    }

    #[test]
    pub fn test_load_index_reader() {
        let _guard = TEST_MUTEX.lock().unwrap();

        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        index_three_column_docs(temp_directory_str);

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
        let _guard = TEST_MUTEX.lock().unwrap();

        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        index_three_column_docs(temp_directory_str);
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
        let _guard = TEST_MUTEX.lock().unwrap();

        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        index_three_column_docs(temp_directory_str);
        assert!(load_index_reader(temp_directory_str).is_ok());

        let res = get_indexed_doc_counts(temp_directory_str);

        // println!("{:?}", res);
        assert!(res.is_ok());
        assert_eq!(res.clone().unwrap(), 5);
    }
}
