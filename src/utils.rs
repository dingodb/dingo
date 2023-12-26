use crate::LOG_CALLBACK;
use crate::logger::ffi_logger::callback_with_thread_info;

use std::{path::Path, fs::{self, File}, io::{Write, Read}};

use serde::{Serialize, Deserialize};
use tantivy::{Index, IndexReader, IndexWriter};

use crate::{commons::{CUSTOM_INDEX_SETTING_FILE_NAME, LOGGER_TARGET}, WARNING};


pub struct IndexR {
    pub path: String,
    pub index: Index,
    pub reader: IndexReader,
}

impl Drop for IndexR {
    fn drop(&mut self) {
        //
    }
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

// Convert Clickhouse like pattern to Rust regex pattern.
pub fn like_to_regex(like_pattern: &str) -> String {
    let mut regex_pattern = String::new();
    let mut escape = false;

    for c in like_pattern.chars() {
        match c {
            // got r'\', if not escape currently, need escape.
            '\\' if !escape => {escape = true;},

            // got r'\', if escaped currently, need push r'\\'
            '\\' if escape => {
                regex_pattern.push_str("\\\\");
                escape = false;
            },

            // In not escape mode, convert '%' to '.*'
            '%' if !escape => regex_pattern.push_str(".*"),

            // In not escape mode, convert '_' to '.'
            '_' if !escape => regex_pattern.push('.'),

            // In escape mode, handle '%'、'_'
            '%' | '_' if escape => {
                regex_pattern.push(c);
                escape = false;
            },

            // Handle regex special chars.
            _ => {
                if ".+*?^$()[]{}|".contains(c) {
                    regex_pattern.push('\\');
                }
                regex_pattern.push(c);
                escape = false;
            },
        }
    }

    regex_pattern
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_like_to_regex() {
        // testing normal strings
        assert_eq!(r"a\bc", "a\\bc");
        assert_eq!(like_to_regex("abc"), "abc");
        assert_eq!(like_to_regex(r"ab\\c"), "ab\\\\c");

        // testing '%' conversion to '.*'
        assert_eq!(like_to_regex(r"a%b%c"), "a.*b.*c");

        // testing '_' conversion to '.'
        assert_eq!(like_to_regex(r"a_b_c"), "a.b.c");

        // testing conversion: '%' and '_'
        assert_eq!(like_to_regex("a\\%b\\_c"), "a%b_c");

        // testing consecutive '%' and '_'
        assert_eq!(like_to_regex(r"%%__"), ".*.*..");

        // testing escape sequences
        assert_eq!(like_to_regex("a\\%b%c\\_d"), "a%b.*c_d");

        // testing escaped '\'
        assert_eq!(like_to_regex("%\\\\%"), ".*\\\\.*");

        // testing special cases such as empty strings
        assert_eq!(like_to_regex(""), "");

        // testing special characters in regular expressions
        assert_eq!(like_to_regex("%a.b[c]%"), ".*a\\.b\\[c\\].*");

        // testing combinations of escaped and unescaped characters.
        assert_eq!(like_to_regex("a%b_c\\%d\\_e\\\\"), "a.*b.c%d_e\\\\");
    }
}
