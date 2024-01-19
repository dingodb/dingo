use std::sync::Arc;

use crate::commons::LOG_CALLBACK;
use crate::logger::ffi_logger::callback_with_thread_info;
use crate::WARNING;
use flurry::HashMap;
use once_cell::sync::Lazy;
use tantivy::{Index, IndexReader};

impl Drop for IndexR {
    fn drop(&mut self) {
        println!("IndexR has been dropped.");
    }
}

pub struct IndexR {
    pub path: String,
    pub index: Index,
    pub reader: IndexReader,
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
    match pinned.get(&key) {
        Some(result) => Ok(result.clone()),
        None => Err(format!(
            "Index Reader doesn't exist with given key: [{}]",
            key
        )),
    }
}

pub fn set_index_r(key: String, value: Arc<IndexR>) -> Result<(), String> {
    let pinned = INDEXR_CACHE.pin();
    if pinned.contains_key(&key) {
        pinned.insert(key.clone(), value.clone());
        WARNING!(
            "{}",
            format!(
                "Index reader already exists with given key: [{}], it has been overwritten.",
                key
            )
        )
    } else {
        pinned.insert(key, value.clone());
    }
    Ok(())
}
pub fn remove_index_r(key: String) -> Result<(), String> {
    let pinned = INDEXR_CACHE.pin();
    if pinned.contains_key(&key) {
        pinned.remove(&key);
    } else {
        WARNING!(
            "{}",
            format!(
                "Index doesn't exist, can't remove it with given key: [{}]",
                key
            )
        )
    }
    Ok(())
}
