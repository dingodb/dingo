use std::{path::Path, fs::{self, File}, io::{Write, Read}};

use serde::{Serialize, Deserialize};
use tantivy::{Index, IndexReader, IndexWriter};

use crate::{commons::{CUSTOM_INDEX_SETTING_FILE_NAME, LOGGER_TARGET}, logger, WARNING};

use logger::ffi_logger::*;

pub struct IndexR {
    pub path: String,
    pub index: Index,
    pub reader: IndexReader,
}

pub struct IndexW {
    pub path: String,
    pub index: Index,
    pub writer: IndexWriter
}

#[derive(Serialize, Deserialize)]
pub struct CustomIndexSetting {
    pub language: String,
}


#[derive(Debug)]
pub enum SearchError {
    NullIndexReader,
    InvalidQueryStr
}

impl std::fmt::Display for SearchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchError::NullIndexReader => write!(f, "IndexReader pointer is null"),
            SearchError::InvalidQueryStr => write!(f, "Invalid query string")
        }
    }
}

/// Before build index, we need prepare this directory.
pub fn prepare_index_directory(path: &Path) -> Result<(), std::io::Error> {
    if path.exists() {
        WARNING!(target: LOGGER_TARGET, "Directory not empty, will remove old data to create index in this directory:{:?}", path);
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)
}

/// Save the custom index settings to a file.
pub fn save_custom_index_setting(path: &Path, setting: &CustomIndexSetting) -> Result<(), std::io::Error> {
    let file_path = path.join(CUSTOM_INDEX_SETTING_FILE_NAME);
    let mut file = File::create(&file_path)?;
    let setting_json = serde_json::to_string(setting).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    file.write_all(setting_json.as_bytes())
}

/// Loads the custom index settings from a file.
pub fn load_custom_index_setting(index_file_path: &Path) -> Result<CustomIndexSetting, std::io::Error> {
    let file_path = index_file_path.join(CUSTOM_INDEX_SETTING_FILE_NAME);
    let mut file = File::open(file_path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    serde_json::from_str(&contents).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
}