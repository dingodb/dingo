#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::merge_policy::LogMergePolicy;
    use tantivy::query::{Query, QueryParser};
    use tantivy::schema::{Schema, FAST, INDEXED, STORED, TEXT};
    use tantivy::{Document, Index, IndexReader, IndexWriter, ReloadPolicy, Term};
    use tempfile::TempDir;

    fn get_reader_and_writer_from_index_path(
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
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()
            .expect("Can't set reload policy");

        (reader, writer)
    }

    fn extract_from_index_reader(
        index_reader: IndexReader,
    ) -> (Field, QueryParser, Box<dyn Query>, Searcher) {
        let text_field = index_reader
            .searcher()
            .index()
            .schema()
            .get_field("text")
            .unwrap();
        let query_parser =
            QueryParser::for_index(&index_reader.searcher().index(), vec![text_field]);
        let text_query = query_parser
            .parse_query("Ancient")
            .expect("Can't parse query");
        (
            text_field,
            query_parser,
            text_query,
            index_reader.searcher(),
        )
    }

    #[test]
    fn test_normal_search() {
        let temp_path = TempDir::new().expect("Can't create temp path");
        let temp_path_str = temp_path.path().to_str().unwrap();
        let (index_reader, _) = get_reader_and_writer_from_index_path(temp_path_str);

        // Prepare variables for search.
        let (text_field, _, text_query, index_searcher) =
            extract_from_index_reader(index_reader.clone());

        let mut top_docs_collector = TopDocsWithFilter::with_limit(10)
            .with_searcher(index_searcher.clone())
            .with_text_field(text_field)
            .with_stored_text(false);

        let mut alive_bitmap = RoaringBitmap::new();
        alive_bitmap.extend(0..3);

        // Not use alive bitmap
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 2);

        // Use alive bitmap
        top_docs_collector = top_docs_collector.with_alive(Arc::new(alive_bitmap));
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 1);
    }

    #[test]
    fn test_search_after_delete() {
        let temp_path = TempDir::new().expect("Can't create temp path");
        let temp_path_str = temp_path.path().to_str().unwrap();
        let (index_reader, mut index_writer) = get_reader_and_writer_from_index_path(temp_path_str);

        // Delete term row_id=0.
        let term: Term = Term::from_field_u64(
            index_writer.index().schema().get_field("row_id").unwrap(),
            0,
        );
        let _ = index_writer.delete_term(term);
        assert!(index_writer.commit().is_ok());

        // Prepare variables for search.
        assert!(index_reader.reload().is_ok());
        let (text_field, _, text_query, index_searcher) =
            extract_from_index_reader(index_reader.clone());

        let top_docs_collector = TopDocsWithFilter::with_limit(10)
            .with_searcher(index_searcher.clone())
            .with_text_field(text_field)
            .with_stored_text(false);

        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 1);
    }

    #[test]
    fn test_boundary_searcher() {
        let temp_path = TempDir::new().expect("Can't create temp path");
        let temp_path_str = temp_path.path().to_str().unwrap();

        // Prepare variables for search.
        let (index_reader, _) = get_reader_and_writer_from_index_path(temp_path_str);
        let (text_field, _, text_query, index_searcher) =
            extract_from_index_reader(index_reader.clone());

        // The Searcher is designed to retrieve documents stored in the index.
        // Use searcher.
        let top_docs_collector = TopDocsWithFilter::with_limit(10)
            .with_searcher(index_searcher.clone())
            .with_text_field(text_field)
            .with_stored_text(true);
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 2);
        assert_eq!(
            searched_results[0].doc,
            "Ancient philosophies provide wisdom for modern dilemmas."
        );
        assert_eq!(
            searched_results[1].doc,
            "Ancient empires rise and fall, shaping history's course."
        );

        // Not use searcher.
        let top_docs_collector = TopDocsWithFilter::with_limit(10)
            .with_text_field(text_field)
            .with_stored_text(true);
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 2);
        assert_eq!(searched_results[0].doc, "");
        assert_eq!(searched_results[1].doc, "");
    }

    #[test]
    fn test_boundary_stored_text() {
        let temp_path = TempDir::new().expect("Can't create temp path");
        let temp_path_str = temp_path.path().to_str().unwrap();

        // Prepare variables for search.
        let (index_reader, _) = get_reader_and_writer_from_index_path(temp_path_str);
        let (text_field, _, text_query, index_searcher) =
            extract_from_index_reader(index_reader.clone());

        // The field `stored_text` can dicide whether to retrieve documents stored in the index.
        // Need stored text.
        let top_docs_collector = TopDocsWithFilter::with_limit(10)
            .with_searcher(index_searcher.clone())
            .with_text_field(text_field)
            .with_stored_text(true);
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 2);
        assert_eq!(
            searched_results[0].doc,
            "Ancient philosophies provide wisdom for modern dilemmas."
        );
        assert_eq!(
            searched_results[1].doc,
            "Ancient empires rise and fall, shaping history's course."
        );

        // Not need stored text.
        let top_docs_collector = TopDocsWithFilter::with_limit(10)
            .with_searcher(index_searcher.clone())
            .with_text_field(text_field)
            .with_stored_text(false);
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 2);
        assert_eq!(searched_results[0].doc, "");
        assert_eq!(searched_results[1].doc, "");
    }

    #[test]
    fn test_boundary_text_field() {
        let temp_path = TempDir::new().expect("Can't create temp path");
        let temp_path_str = temp_path.path().to_str().unwrap();

        // Prepare variables for search.
        let (index_reader, _) = get_reader_and_writer_from_index_path(temp_path_str);
        let (text_field, _, text_query, index_searcher) =
            extract_from_index_reader(index_reader.clone());

        // If need to retrieve stored doc, we will find it under field `text_field`, if field `text_filed` is none, we won't load stored doc.
        // With `text_field`.
        let top_docs_collector = TopDocsWithFilter::with_limit(10)
            .with_searcher(index_searcher.clone())
            .with_text_field(text_field)
            .with_stored_text(true);
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 2);
        assert_eq!(
            searched_results[0].doc,
            "Ancient philosophies provide wisdom for modern dilemmas."
        );
        assert_eq!(
            searched_results[1].doc,
            "Ancient empires rise and fall, shaping history's course."
        );

        // Without `text_field`
        let top_docs_collector = TopDocsWithFilter::with_limit(10)
            .with_searcher(index_searcher.clone())
            .with_stored_text(true);
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 2);
        assert_eq!(searched_results[0].doc, "");
        assert_eq!(searched_results[1].doc, "");
    }

    #[test]
    fn test_boundary_limit() {
        let temp_path = TempDir::new().expect("Can't create temp path");
        let temp_path_str = temp_path.path().to_str().unwrap();

        // Prepare variables for search.
        let (index_reader, _) = get_reader_and_writer_from_index_path(temp_path_str);
        let (text_field, _, text_query, index_searcher) =
            extract_from_index_reader(index_reader.clone());

        // The field `limit` can decide how many results will be returned.
        // limit size = 3.
        let top_docs_collector = TopDocsWithFilter::with_limit(3)
            .with_searcher(index_searcher.clone())
            .with_text_field(text_field)
            .with_stored_text(true);
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 2);
        assert_eq!(
            searched_results[0].doc,
            "Ancient philosophies provide wisdom for modern dilemmas."
        );
        assert_eq!(
            searched_results[1].doc,
            "Ancient empires rise and fall, shaping history's course."
        );
        // limit size = 1.
        let top_docs_collector = TopDocsWithFilter::with_limit(1)
            .with_searcher(index_searcher.clone())
            .with_text_field(text_field)
            .with_stored_text(true);
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 1);
        assert_eq!(
            searched_results[0].doc,
            "Ancient philosophies provide wisdom for modern dilemmas."
        );

        // limit size = 0.
        let top_docs_collector = TopDocsWithFilter::with_limit(0)
            .with_searcher(index_searcher.clone())
            .with_text_field(text_field)
            .with_stored_text(false);
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 0);
    }
}
