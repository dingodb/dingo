#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::common::tests::index_3column_docs_with_threads_merge;
    use crate::search::implements::api_common_impl::load_index_reader;
    use crate::search::implements::api_myscale_impl::bm25_search;

    #[test]
    pub fn test_bm25_search() {
        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        index_3column_docs_with_threads_merge(temp_directory_str);
        assert!(load_index_reader(temp_directory_str).is_ok());

        let res = bm25_search(temp_directory_str, "Ancient", 10, &vec![], false);

        assert!(res.is_ok());
        assert_eq!(res.clone().unwrap().len(), 2);
        assert_eq!(res.clone().unwrap()[0].row_id, 4);
        assert_eq!(res.clone().unwrap()[1].row_id, 0);
        assert!(res.clone().unwrap()[0].score >= 0.85);
        assert!(res.clone().unwrap()[1].score <= 0.81);
    }
}
