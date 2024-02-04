use std::sync::Arc;

use crate::logger::ffi_logger::callback_with_thread_info;
use crate::{commons::LOG_CALLBACK, INFO, WARNING};
use flurry::HashMap;
use once_cell::sync::{Lazy, OnceCell};
use tantivy::{Executor, Index, IndexReader};

pub struct IndexR {
    pub path: String,
    pub index: Index,
    pub reader: IndexReader,
}

impl Drop for IndexR {
    fn drop(&mut self) {
        INFO!("IndexR has been dropped. index_path:[{}]", self.path);
    }
}

impl IndexR {
    pub fn reader_address(&self) -> usize {
        &self.reader as *const IndexReader as usize
    }
    pub fn reload(&self) -> Result<(), String> {
        self.reader.reload().map_err(|e| e.to_string())
    }
}

// cache store IndexW for thread safe
static INDEXR_CACHE: Lazy<Arc<HashMap<String, Arc<IndexR>>>> =
    Lazy::new(|| Arc::new(HashMap::new()));

pub fn get_index_r(key: String) -> Result<Arc<IndexR>, String> {
    let pinned = INDEXR_CACHE.pin();
    let trimmed_key: String = key.trim_end_matches('/').to_string();
    match pinned.get(&trimmed_key) {
        Some(result) => Ok(result.clone()),
        None => Err(format!(
            "Index Reader doesn't exist with given key: [{}]",
            trimmed_key
        )),
    }
}

pub fn set_index_r(key: String, value: Arc<IndexR>) -> Result<(), String> {
    let pinned = INDEXR_CACHE.pin();
    let trimmed_key: String = key.trim_end_matches('/').to_string();
    if pinned.contains_key(&trimmed_key) {
        pinned.insert(trimmed_key.clone(), value.clone());
        WARNING!(
            "{}",
            format!(
                "Index reader already exists with given key: [{}], it has been overwritten.",
                trimmed_key
            )
        )
    } else {
        pinned.insert(trimmed_key, value.clone());
    }
    Ok(())
}

pub fn remove_index_r(key: String) -> Result<(), String> {
    let pinned = INDEXR_CACHE.pin();
    let trimmed_key: String = key.trim_end_matches('/').to_string();
    if pinned.contains_key(&trimmed_key) {
        pinned.remove(&trimmed_key);
    } else {
        let error_info: String = format!("IndexR with given key [{}] already removed", trimmed_key);
        WARNING!("{}", error_info);
        return Err(error_info);
    }
    Ok(())
}

// shared thread pool for index searching.
static INDEX_SHARED_THREAD_POOL: OnceCell<Arc<Executor>> = OnceCell::new();

pub fn get_shared_multithread_executor(num_threads: usize) -> Result<Arc<Executor>, String> {
    let res: Result<&Arc<Executor>, String> = INDEX_SHARED_THREAD_POOL.get_or_try_init(|| {
        Executor::multi_thread(num_threads, "tantivy-search-")
            .map(Arc::new)
            .map_err(|e| e.to_string())
    });

    res.map(|executor| executor.clone())
}
