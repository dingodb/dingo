use std::sync::Arc;

use crate::logger::logger_bridge::TantivySearchLogger;
use crate::ERROR;
use crate::{common::constants::LOG_CALLBACK, WARNING};
use flurry::HashMap;
use once_cell::sync::OnceCell;
use tantivy::Executor;

use super::index_reader_bridge::IndexReaderBridge;

pub struct IndexReaderBridgeCache {
    cache: HashMap<String, Arc<IndexReaderBridge>>,
    shared_thread_pool: OnceCell<Arc<Executor>>,
}

impl IndexReaderBridgeCache {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
            shared_thread_pool: OnceCell::new(),
        }
    }

    pub fn set_index_reader_bridge(
        &self,
        key: String,
        value: Arc<IndexReaderBridge>,
    ) -> Result<(), String> {
        let pinned = self.cache.pin();
        let trimmed_key: String = key.trim_end_matches('/').to_string();
        if pinned.contains_key(&trimmed_key) {
            pinned.insert(trimmed_key.clone(), value.clone());
            WARNING!(
                "{}",
                format!(
                    "IndexReaderBridge already exists with given key: [{}], it has been overwritten.",
                    trimmed_key
                )
            )
        } else {
            pinned.insert(trimmed_key, value.clone());
        }
        Ok(())
    }

    pub fn get_index_reader_bridge(&self, key: String) -> Result<Arc<IndexReaderBridge>, String> {
        let pinned = self.cache.pin();
        let trimmed_key: String = key.trim_end_matches('/').to_string();
        match pinned.get(&trimmed_key) {
            Some(result) => Ok(result.clone()),
            None => Err(format!(
                "IndexReaderBridge doesn't exist with given key: [{}]",
                trimmed_key
            )),
        }
    }

    pub fn remove_index_reader_bridge(&self, key: String) -> Result<(), String> {
        let pinned = self.cache.pin();
        let trimmed_key: String = key.trim_end_matches('/').to_string();
        if pinned.contains_key(&trimmed_key) {
            pinned.remove(&trimmed_key);
        } else {
            let error_info: String = format!(
                "IndexReaderBridge doesn't exist, can't remove it with given key [{}]",
                trimmed_key
            );
            ERROR!("{}", error_info);
            return Err(error_info);
        }
        Ok(())
    }

    // shared thread pool for index searcher.
    pub fn get_shared_multithread_executor(
        &self,
        num_threads: usize,
    ) -> Result<Arc<Executor>, String> {
        if num_threads <= 0 {
            return Err("threads number minimum is 1".to_string());
        }
        let res: Result<&Arc<Executor>, String> = self.shared_thread_pool.get_or_try_init(|| {
            Executor::multi_thread(num_threads, "tantivy-search-")
                .map(Arc::new)
                .map_err(|e| e.to_string())
        });

        res.map(|executor| executor.clone())
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use tantivy::{
        merge_policy::LogMergePolicy,
        schema::{Schema, FAST, INDEXED, STORED, TEXT},
        Document, Index,
    };
    use tempfile::TempDir;

    use crate::search::bridge::{
        index_reader_bridge::IndexReaderBridge, index_reader_bridge_cache::IndexReaderBridgeCache,
    };

    fn create_index_reader_bridge(index_directory_str: &str) -> IndexReaderBridge {
        // Construct the schema for the index.
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("row_id", FAST | INDEXED);
        schema_builder.add_text_field("text", TEXT | STORED);
        let schema = schema_builder.build();
        // Create the index in the specified directory.
        let index = Index::create_in_dir(index_directory_str.to_string(), schema.clone())
            .expect("Can't create index");
        // Create the writer with a specified buffer size (e.g., 64 MB).
        let writer = index
            .writer_with_num_threads(2, 1024 * 1024 * 64)
            .expect("Can't create index writer");
        // Configure default merge policy
        writer.set_merge_policy(Box::new(LogMergePolicy::default()));
        // Index some documents.
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
            let result = writer.add_document(doc);
            assert!(result.is_ok());
        }
        IndexReaderBridge {
            path: index_directory_str.to_string(),
            index: index.clone(),
            reader: index.reader().expect("Can't get reader from index"),
        }
    }

    #[test]
    fn test_set_index_reader_bridge() {
        let test_cache = IndexReaderBridgeCache::new();
        // Create two temp directory for test.
        let path_a = TempDir::new().expect("Can't create temp directory");
        let path_b = TempDir::new().expect("Can't create temp directory");
        let path_a_str = path_a.path().to_str().unwrap();
        let path_b_str = path_b.path().to_str().unwrap();

        let index_value_a = create_index_reader_bridge(path_a_str);
        let first_inserted =
            test_cache.set_index_reader_bridge(path_a_str.to_string(), Arc::new(index_value_a));
        assert!(first_inserted.is_ok());

        let index_value_b = create_index_reader_bridge(path_b_str);
        let second_inserted =
            test_cache.set_index_reader_bridge(path_b_str.to_string(), Arc::new(index_value_b));
        assert!(second_inserted.is_ok());
    }

    #[test]
    fn test_get_and_set_index_reader_bridge() {
        let test_cache = IndexReaderBridgeCache::new();

        // Create two temp directory for test.
        let path_a = TempDir::new().expect("Can't create temp path_a");
        let path_b = TempDir::new().expect("Can't create temp path_b");
        let path_a_str = path_a.path().to_str().unwrap();
        let path_b_str = path_b.path().to_str().unwrap();

        // Insert value `index_reader_bridge_a` with the key `path_a_str` into cache.
        let index_reader_bridge_a = Arc::new(create_index_reader_bridge(path_a_str));
        let first_set =
            test_cache.set_index_reader_bridge(path_a_str.to_string(), index_reader_bridge_a);
        assert!(first_set.is_ok());
        let first_get = test_cache.get_index_reader_bridge(path_a_str.to_string());
        assert!(first_get.is_ok());
        assert_eq!(first_get.unwrap().path, path_a_str.to_string());

        // Boundary test for `get_index_writer_bridge`.
        let expect_error_result = test_cache.get_index_reader_bridge("not_exists".to_string());
        assert!(expect_error_result.is_err());

        // Testing whether the cache can update the value for the same key (`path_a_str`).
        let index_reader_bridge_b = Arc::new(create_index_reader_bridge(path_b_str));
        let second_set =
            test_cache.set_index_reader_bridge(path_a_str.to_string(), index_reader_bridge_b);
        assert!(second_set.is_ok());
        let second_get = test_cache.get_index_reader_bridge(path_a_str.to_string());
        assert!(second_get.is_ok());
        assert_eq!(second_get.unwrap().path, path_b_str.to_string());
    }

    #[test]
    fn test_remove_index_reader_bridge() {
        let test_cache = IndexReaderBridgeCache::new();

        // Create two temp directory for test.
        let path = TempDir::new().expect("Can't create temp directory");
        let path_str = path.path().to_str().unwrap();

        // Cache value `index_reader_bridge` with the key `path_str`.
        let index_reader_bridge = Arc::new(create_index_reader_bridge(path_str));
        let first_inserted =
            test_cache.set_index_reader_bridge(path_str.to_string(), index_reader_bridge.clone());
        assert!(first_inserted.is_ok());
        let first_get = test_cache.get_index_reader_bridge(path_str.to_string());
        assert!(first_get.is_ok());
        assert_eq!(first_get.unwrap().path, path_str.to_string());

        // Remove `index_reader_bridge`
        let get_before_remove = test_cache.get_index_reader_bridge(path_str.to_string());
        assert!(get_before_remove.is_ok());
        let first_removed = test_cache.remove_index_reader_bridge(path_str.to_string());
        assert!(first_removed.is_ok());
        let get_after_remove = test_cache.get_index_reader_bridge(path_str.to_string());
        assert!(get_after_remove.is_err());

        // Remove a not exist `IndexReaderBridge` will trigger an error.
        let second_removed = test_cache.remove_index_reader_bridge(path_str.to_string());
        assert!(second_removed.is_err());
    }

    #[test]
    fn test_shared_multithread_executor() {
        let test_cache = IndexReaderBridgeCache::new();

        assert!(test_cache.get_shared_multithread_executor(0).is_err());
        let executor1 = test_cache
            .get_shared_multithread_executor(2)
            .expect("Failed to get executor for the first time");
        let executor2 = test_cache
            .get_shared_multithread_executor(4)
            .expect("Failed to get executor for the second time");

        assert!(
            Arc::ptr_eq(&executor1, &executor2),
            "Executors should be the same instance"
        );
    }
}
