#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::common::tests::index_3column_docs_with_threads_merge;
    use crate::search::implements::api_clickhouse_impl::{
        query_sentence_bitmap, query_sentence_with_range, query_term_bitmap, query_term_with_range,
        query_terms_bitmap, query_terms_with_range, regex_term_bitmap, regex_term_with_range,
    };
    use crate::search::implements::api_common_impl::load_index_reader;

    #[test]
    pub fn test_query_term_with_range() {
        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        index_3column_docs_with_threads_merge(temp_directory_str);
        assert!(load_index_reader(temp_directory_str).is_ok());

        let res = query_term_with_range(temp_directory_str, "col1", "Ancient", 0, 1);

        assert!(res.is_ok());
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    pub fn test_query_terms_with_range() {
        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        index_3column_docs_with_threads_merge(temp_directory_str);
        assert!(load_index_reader(temp_directory_str).is_ok());

        let res = query_terms_with_range(
            temp_directory_str,
            "col1",
            &vec!["Ancient".to_string(), "Social".to_string()],
            1,
            1,
        );

        assert!(res.is_ok());
        assert_eq!(res.unwrap(), false)
    }

    #[test]
    pub fn test_query_sentence_with_range() {
        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        index_3column_docs_with_threads_merge(temp_directory_str);
        assert!(load_index_reader(temp_directory_str).is_ok());

        let res = query_sentence_with_range(
            temp_directory_str,
            "col1",
            "Artistic expressions reflect diverse cultural heritages.",
            1,
            1,
        );

        assert!(res.is_ok());
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    pub fn test_regex_term_with_range() {
        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        index_3column_docs_with_threads_merge(temp_directory_str);
        assert!(load_index_reader(temp_directory_str).is_ok());

        let res = regex_term_with_range(temp_directory_str, "col1", "%pressio%", 1, 1);

        assert!(res.is_ok());
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    pub fn test_query_term_bitmap() {
        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        index_3column_docs_with_threads_merge(temp_directory_str);
        assert!(load_index_reader(temp_directory_str).is_ok());

        let res = query_term_bitmap(temp_directory_str, "col1", "Ancient");

        assert!(res.is_ok());
        assert_eq!(res.clone().unwrap().len(), 1);
        assert_eq!(res.unwrap()[0], 17);
    }

    #[test]
    pub fn test_query_terms_bitmap() {
        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        index_3column_docs_with_threads_merge(temp_directory_str);
        assert!(load_index_reader(temp_directory_str).is_ok());

        let res = query_terms_bitmap(
            temp_directory_str,
            "col1",
            &vec!["Ancient".to_string(), "Social".to_string()],
        );

        // println!("{:?}", res);
        assert!(res.is_ok());
        assert_eq!(res.clone().unwrap().len(), 1);
        assert_eq!(res.unwrap()[0], 21);
    }

    #[test]
    pub fn test_query_sentence_bitmap() {
        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        index_3column_docs_with_threads_merge(temp_directory_str);
        assert!(load_index_reader(temp_directory_str).is_ok());

        let res = query_sentence_bitmap(
            temp_directory_str,
            "col1",
            "Artistic expressions reflect diverse cultural heritages.",
        );

        // println!("{:?}", res);
        assert!(res.is_ok());
        assert_eq!(res.clone().unwrap().len(), 1);
        assert_eq!(res.unwrap()[0], 2);
    }

    #[test]
    pub fn test_regex_term_bitmap() {
        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        index_3column_docs_with_threads_merge(temp_directory_str);
        assert!(load_index_reader(temp_directory_str).is_ok());

        let res = regex_term_bitmap(temp_directory_str, "col1", "%pressio%");

        assert!(res.is_ok());
        assert_eq!(res.clone().unwrap().len(), 1);
        assert_eq!(res.unwrap()[0], 2);
    }
}
