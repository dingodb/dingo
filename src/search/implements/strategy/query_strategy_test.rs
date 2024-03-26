#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use roaring::RoaringBitmap;

    use tempfile::TempDir;

    use crate::{
        common::tests::index_3column_docs_with_threads_merge,
        ffi::RowIdWithScore,
        search::implements::strategy::query_strategy::{
            BM25QueryStrategy, ParserQueryStrategy, QueryExecutor, RegexQueryStrategy,
            SingleTermQueryStrategy, TermSetQueryStrategy,
        },
    };

    #[test]
    fn test_term_set_query_strategy() {
        let temp_directory: TempDir = TempDir::new().unwrap();
        let temp_directory_str: &str = temp_directory.path().to_str().unwrap();
        let (index_reader, _) = index_3column_docs_with_threads_merge(temp_directory_str);

        // Choose query strategy to construct query executor.
        let terms_query: TermSetQueryStrategy<'_> = TermSetQueryStrategy {
            terms: &["ancient".to_string(), "balance".to_string()].to_vec(),
            column_name: "col1",
        };
        let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> =
            QueryExecutor::new(&terms_query);

        // Compute query results.
        let result: Arc<RoaringBitmap> = query_executor.execute(&index_reader.searcher()).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_single_term_query_strategy() {
        let temp_directory: TempDir = TempDir::new().unwrap();
        let temp_directory_str: &str = temp_directory.path().to_str().unwrap();
        let (index_reader, _) = index_3column_docs_with_threads_merge(temp_directory_str);

        // Choose query strategy to construct query executor.
        let term_query: SingleTermQueryStrategy<'_> = SingleTermQueryStrategy {
            term: "judgment",
            column_name: "col2",
        };
        let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&term_query);

        // Compute query results.
        let result: Arc<RoaringBitmap> = query_executor.execute(&index_reader.searcher()).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_regex_query_strategy() {
        let temp_directory: TempDir = TempDir::new().unwrap();
        let temp_directory_str: &str = temp_directory.path().to_str().unwrap();
        let (index_reader, _) = index_3column_docs_with_threads_merge(temp_directory_str);

        // Choose query strategy to construct query executor.
        let regex_query: RegexQueryStrategy<'_> = RegexQueryStrategy {
            column_name: "col2",
            pattern: "%dgmen%",
        };
        let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> =
            QueryExecutor::new(&regex_query);

        // Compute query results.
        let result: Arc<RoaringBitmap> = query_executor.execute(&index_reader.searcher()).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_parser_query_strategy() {
        let temp_directory: TempDir = TempDir::new().unwrap();
        let temp_directory_str: &str = temp_directory.path().to_str().unwrap();
        let (index_reader, _) = index_3column_docs_with_threads_merge(temp_directory_str);

        // Choose query strategy to construct query executor.
        let regex_query: ParserQueryStrategy<'_> = ParserQueryStrategy {
            column_name: "col3",
            sentence: "Literary inventions capture philosophical masterpieces.",
        };
        let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> =
            QueryExecutor::new(&regex_query);

        // Compute query results.
        let result: Arc<RoaringBitmap> = query_executor.execute(&index_reader.searcher()).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_bm25_query_strategy() {
        let temp_directory: TempDir = TempDir::new().unwrap();
        let temp_directory_str: &str = temp_directory.path().to_str().unwrap();
        let (index_reader, _) = index_3column_docs_with_threads_merge(temp_directory_str);

        // Choose query strategy to construct query executor.
        let bm25_strategy: BM25QueryStrategy<'_> = BM25QueryStrategy {
            sentence: "Literary inventions capture philosophical masterpieces.",
            topk: &10,
            query_with_filter: &false,
            u8_aived_bitmap: &vec![],
        };
        let query_executor: QueryExecutor<'_, Vec<RowIdWithScore>> =
            QueryExecutor::new(&bm25_strategy);

        // Compute query results.
        let result: Vec<RowIdWithScore> = query_executor.execute(&index_reader.searcher()).unwrap();

        assert_eq!(result[0].row_id, 2);
        assert!(result[0].score >= 4.0);
        assert_eq!(result[1].row_id, 0);
        assert!(result[1].score <= 1.6);
    }
}
