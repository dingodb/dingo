
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use roaring::RoaringBitmap;
    use tantivy::{
        merge_policy::LogMergePolicy, schema::{Schema, FAST, INDEXED, STORED, TEXT}, Document, Index, IndexReader, IndexWriter, ReloadPolicy,
    };
    use tempfile::TempDir;

    use crate::search::implements::strategy::query_strategy::{QueryExecutor, TermSetQueryStrategy};

    fn index_some_docs_in_temp_directory(
        index_directory_str: &str,
    ) -> (IndexReader, IndexWriter) {
        // Construct the schema for the index.
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("row_id", FAST | INDEXED);
        schema_builder.add_text_field("text", TEXT | STORED);
        let schema = schema_builder.build();
        // Create the index in the specified directory.
        let index = Index::create_in_dir(index_directory_str.to_string(), schema.clone())
            .expect("Can't create index");
        // Create the writer with a specified buffer size (e.g., 64 MB).
        let mut writer = index
            .writer_with_num_threads(2, 1024 * 1024 * 64)
            .expect("Can't create index writer");
        // Configure default merge policy.
        writer.set_merge_policy(Box::new(LogMergePolicy::default()));
        // Index some docs.
        let docs: Vec<String> = vec![
            "Ancient empires rise and fall, shaping history's course.".to_string(),
            "Artistic expressions reflect diverse cultural heritages.".to_string(),
            "Social movements transform societies, forging new paths.".to_string(),
            "Strategic military campaigns alter the balance of power.".to_string(),
            "Ancient philosophies provide wisdom for modern dilemmas.".to_string(),
        ];
        for row_id in 0..docs.len() {
            let mut doc = Document::default();
            doc.add_u64(schema.get_field("row_id").unwrap(), row_id as u64);
            doc.add_text(schema.get_field("text").unwrap(), &docs[row_id]);
            assert!(writer.add_document(doc).is_ok());
        }
        assert!(writer.commit().is_ok());

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .expect("Can't set reload policy");
        (reader, writer)
    }



    #[test]
    fn test_term_set_query_strategy() {
        let temp_directory = TempDir::new().expect("Can't create temp directory");
        let temp_directory_str = temp_directory
            .path()
            .to_str()
            .expect("Can't get temp directory str");
        let (index_reader, _) =
            index_some_docs_in_temp_directory(temp_directory_str);
        // Choose query strategy to construct query executor.
        let terms_query: TermSetQueryStrategy<'_> = TermSetQueryStrategy {
            terms: &[
                "ancient".to_string(),
                "balance".to_string()
                ].to_vec(),
            column_name: "text",
        };
        let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&terms_query);
        // Compute query results.
        let result: Arc<RoaringBitmap> = query_executor.execute(&index_reader.searcher()).unwrap();
        assert_eq!(result.len(), 3);
    }
}
