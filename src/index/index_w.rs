use std::sync::{Arc, Mutex};

use crate::{commons::LOG_CALLBACK, INFO};
use crate::logger::ffi_logger::callback_with_thread_info;
use crate::WARNING;
use flurry::HashMap;
use once_cell::sync::Lazy;
use tantivy::{Document, Index, IndexWriter, Opstamp, Term};

pub struct IndexW {
    pub path: String,
    pub index: Index,
    pub writer: Mutex<Option<IndexWriter>>,
}

impl IndexW {
    // wrapper for IndexWriter.commit
    pub fn commit(&self) -> Result<Opstamp, String> {
        match self.writer.lock() {
            Ok(mut writer) => {
                if let Some(writer) = writer.as_mut() {
                    writer.commit().map_err(|e| e.to_string())
                } else {
                    Err("IndexWriter is not available".to_string())
                }
            }
            Err(e) => Err(format!("Lock error: {}", e)),
        }
    }

    // wrapper for IndexWriter.add_document
    pub fn add_document(&self, document: Document) -> Result<Opstamp, String> {
        match self.writer.lock() {
            Ok(mut writer) => {
                if let Some(writer) = writer.as_mut() {
                    writer.add_document(document).map_err(|e| e.to_string())
                } else {
                    Err("IndexWriter is not available".to_string())
                }
            }
            Err(e) => Err(format!("Lock error: {}", e)),
        }
    }

    // wrapper for IndexWriter.delete_term
    #[deprecated]
    #[allow(dead_code)]
    pub fn delete_term(&self, term: Term) -> Result<Opstamp, String> {
        match self.writer.lock() {
            Ok(mut writer) => {
                if let Some(writer) = writer.as_mut() {
                    Ok(writer.delete_term(term))
                } else {
                    Err("IndexWriter is not available for delete_term".to_string())
                }
            }
            Err(e) => Err(format!("Lock error: {}", e)),
        }
    }

    // wrapper for IndexWriter.delete_term
    pub fn delete_terms(&self, terms: Vec<Term>) -> Result<Opstamp, String> {
        match self.writer.lock() {
            Ok(mut writer) => {
                if let Some(writer) = writer.as_mut() {
                    let mut opstamp: Opstamp = 0;
                    for term in terms {
                        opstamp = writer.delete_term(term)
                    }
                    Ok(opstamp)
                } else {
                    Err("IndexWriter is not available for delete_term".to_string())
                }
            }
            Err(e) => Err(format!("Lock error: {}", e)),
        }
    }

    // wrapper for IndexWriter.wait_merging_threads.
    pub fn wait_merging_threads(&self) -> Result<(), String> {
        // use Interior Mutability
        match self.writer.lock() {
            Ok(mut writer) => {
                if let Some(writer) = writer.take() {
                    let _ = writer.wait_merging_threads();
                };
                Ok(())
            }
            Err(e) => Err(format!("Failed to acquire lock in drop: {}", e.to_string())),
        }
    }
}

impl Drop for IndexW {
    fn drop(&mut self) {
        INFO!("IndexW has been dropped. index_path:[{}]", self.path);
    }
}

// cache store IndexW for thread safe
static INDEXW_CACHE: Lazy<Arc<HashMap<String, Arc<IndexW>>>> =
    Lazy::new(|| Arc::new(HashMap::new()));

pub fn get_index_w(key: String) -> Result<Arc<IndexW>, String> {
    let pinned = INDEXW_CACHE.pin();
    match pinned.get(&key) {
        Some(result) => Ok(result.clone()),
        None => Err(format!(
            "Index Writer doesn't exist with given key: [{}]",
            key
        )),
    }
}

pub fn set_index_w(key: String, value: Arc<IndexW>) -> Result<(), String> {
    let pinned = INDEXW_CACHE.pin();
    if pinned.contains_key(&key) {
        pinned.insert(key.clone(), value.clone());
        WARNING!(
            "{}",
            format!(
                "Index writer already exists with given key: [{}], it has been overwritten.",
                key
            )
        )
    } else {
        pinned.insert(key, value.clone());
    }
    Ok(())
}
pub fn remove_index_w(key: String) -> Result<(), String> {
    let pinned = INDEXW_CACHE.pin();
    if pinned.contains_key(&key) {
        pinned.remove(&key);
    } else {
        WARNING!(
            "{}",
            format!(
                "IndexW doesn't exist, can't remove it with given key: [{}]",
                key
            )
        )
    }
    Ok(())
}
