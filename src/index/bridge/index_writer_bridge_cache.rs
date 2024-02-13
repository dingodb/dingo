use super::index_writer_bridge::IndexWriterBridge;
use crate::logger::ffi_logger::callback_with_thread_info;
use crate::{common::constants::LOG_CALLBACK, WARNING};
use flurry::HashMap;
use std::sync::Arc;

pub struct IndexWriterBridgeCache {
    cache: HashMap<String, Arc<IndexWriterBridge>>,
}

impl IndexWriterBridgeCache {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    // TODO: trimmed need to be done in FFI entry.
    pub fn get_index_writer_bridge(&self, key: String) -> Result<Arc<IndexWriterBridge>, String> {
        let pinned = self.cache.pin();
        let trimmed_key: String = key.trim_end_matches('/').to_string();
        match pinned.get(&trimmed_key) {
            Some(result) => Ok(result.clone()),
            None => Err(format!(
                "Index Writer doesn't exist with given key: [{}]",
                trimmed_key
            )),
        }
    }

    pub fn set_index_writer_bridge(
        &self,
        key: String,
        value: Arc<IndexWriterBridge>,
    ) -> Result<(), String> {
        let pinned = self.cache.pin();
        let trimmed_key: String = key.trim_end_matches('/').to_string();
        if pinned.contains_key(&trimmed_key) {
            pinned.insert(trimmed_key.clone(), value.clone());
            WARNING!(
                "{}",
                format!(
                    "Index writer already exists with given key: [{}], it has been overwritten.",
                    trimmed_key
                )
            )
        } else {
            pinned.insert(trimmed_key, value.clone());
        }
        Ok(())
    }
    pub fn remove_index_writer_bridge(&self, key: String) -> Result<(), String> {
        let pinned = self.cache.pin();
        let trimmed_key: String = key.trim_end_matches('/').to_string();
        if pinned.contains_key(&trimmed_key) {
            pinned.remove(&trimmed_key);
        } else {
            let warning = format!(
                "IndexWriterBridge doesn't exist, can't remove it with given key: [{}]",
                trimmed_key
            );
            WARNING!("{}", warning)
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::index::bridge::index_writer_bridge::IndexWriterBridge;
    use crate::index::bridge::index_writer_bridge_cache::IndexWriterBridgeCache;
    use std::sync::{Arc, Mutex};
    use tantivy::{
        merge_policy::LogMergePolicy,
        schema::{Schema, FAST, INDEXED, STORED, TEXT},
        Index,
    };
    use tempfile::TempDir;

    fn create_index_in_temp_directory(index_directory_str: &str) -> IndexWriterBridge {
        // Construct the schema for the index.
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("row_id", FAST | INDEXED);
        schema_builder.add_text_field("text", TEXT | STORED);
        let schema = schema_builder.build();
        // Create the index in the specified directory.
        let index = Index::create_in_dir(index_directory_str.to_string(), schema)
            .expect("Can't create index");
        // Create the writer with a specified buffer size (e.g., 64 MB).
        let writer = index
            .writer_with_num_threads(2, 1024 * 1024 * 64)
            .expect("Can't create index writer");
        // Configure default merge policy
        writer.set_merge_policy(Box::new(LogMergePolicy::default()));
        // Generate indexW.
        let index_w = IndexWriterBridge {
            index,
            path: index_directory_str.to_string(),
            writer: Mutex::new(Some(writer)),
        };
        index_w
    }

    #[test]
    fn test_set_index_writer_bridge() {
        let test_cache = IndexWriterBridgeCache::new();
        // Create two temp directory for test.
        let path_a = TempDir::new().expect("Can't create temp directory");
        let path_b = TempDir::new().expect("Can't create temp directory");
        let path_a_str = path_a.path().to_str().unwrap();
        let path_b_str = path_b.path().to_str().unwrap();

        let index_value_a = create_index_in_temp_directory(path_a_str);
        let first_inserted =
            test_cache.set_index_writer_bridge(path_a_str.to_string(), Arc::new(index_value_a));
        assert!(first_inserted.is_ok());

        let index_value_b = create_index_in_temp_directory(path_b_str);
        let second_inserted =
            test_cache.set_index_writer_bridge(path_b_str.to_string(), Arc::new(index_value_b));
        assert!(second_inserted.is_ok());
    }

    #[test]
    fn test_get_and_set_index_writer_bridge() {
        let test_cache = IndexWriterBridgeCache::new();

        // Create two temp directory for test.
        let path_a = TempDir::new().expect("Can't create temp directory");
        let path_b = TempDir::new().expect("Can't create temp directory");
        let path_a_str = path_a.path().to_str().unwrap();
        let path_b_str = path_b.path().to_str().unwrap();

        // Insert value `index_value_a` with the key `path_a_str` into cache.
        let index_value_a = Arc::new(create_index_in_temp_directory(path_a_str));
        let first_inserted =
            test_cache.set_index_writer_bridge(path_a_str.to_string(), index_value_a);
        assert!(first_inserted.is_ok());

        let first_get = test_cache.get_index_writer_bridge(path_a_str.to_string());
        assert!(first_get.is_ok());
        assert_eq!(first_get.unwrap().path, path_a_str.to_string());

        // Boundary test for `get_index_writer_bridge`.
        let expect_error_result = test_cache.get_index_writer_bridge("not_exists".to_string());
        assert!(expect_error_result.is_err());

        // Testing whether the cache can update the value for the same key (`path_a_str`).
        let index_value_b = Arc::new(create_index_in_temp_directory(path_b_str));
        let second_inserted =
            test_cache.set_index_writer_bridge(path_a_str.to_string(), index_value_b);
        assert!(second_inserted.is_ok());
        let second_get = test_cache.get_index_writer_bridge(path_a_str.to_string());
        assert!(second_get.is_ok());
        assert_eq!(second_get.unwrap().path, path_b_str.to_string());
    }

    #[test]
    fn test_remove_index_writer_bridge() {
        let test_cache = IndexWriterBridgeCache::new();

        // Create two temp directory for test.
        let path = TempDir::new().expect("Can't create temp directory");
        let path_str = path.path().to_str().unwrap();

        // Insert value `index_value` with the key `path_str` into cache.
        let index_value = Arc::new(create_index_in_temp_directory(path_str));
        let first_inserted =
            test_cache.set_index_writer_bridge(path_str.to_string(), index_value.clone());
        assert!(first_inserted.is_ok());
        let first_get = test_cache.get_index_writer_bridge(path_str.to_string());
        assert!(first_get.is_ok());
        assert_eq!(first_get.unwrap().path, path_str.to_string());

        // Remove `index_value`
        let get_before_remove = test_cache.get_index_writer_bridge(path_str.to_string());
        assert!(get_before_remove.is_ok());
        let first_removed = test_cache.remove_index_writer_bridge(path_str.to_string());
        assert!(first_removed.is_ok());
        let get_after_remove = test_cache.get_index_writer_bridge(path_str.to_string());
        assert!(get_after_remove.is_err());

        // Remove a not exist `IndexWriterBridge` will not trigger error.
        let second_removed = test_cache.remove_index_writer_bridge(path_str.to_string());
        assert!(second_removed.is_ok());
    }
}
