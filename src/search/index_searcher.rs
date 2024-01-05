
use std::{sync::Arc, path::Path};
use cxx::CxxString;
use crate::ERROR;
use crate::tokenizer::parse_and_register::get_custom_tokenizer;
use crate::tokenizer::parse_and_register::register_tokenizer_to_index;
use crate::commons::LOG_CALLBACK;
use crate::logger::ffi_logger::callback_with_thread_info;

use super::index_r::*;
use super::utils::perform_search;
use crate::common::index_utils::*;

use tantivy::{Index, ReloadPolicy};



/// Loads an index from a specified directory.
///
/// Arguments:
/// - `index_path`: The directory path for building the index.
///
/// Returns:
/// - A bool value represent operation success.
pub fn tantivy_load_index(index_path: &CxxString) -> Result<bool, String> {
    // Parse parameter.
    let index_path_str = match index_path.to_str() {
        Ok(content) => content.to_string(),
        Err(e) => {
            return Err(format!("Can't parse parameter index_path: {}, exception: {}", index_path, e.to_string()));
        }
    };

    // Verify index files directory.
    let index_files_directory = Path::new(&index_path_str);
    if !index_files_directory.exists() || !index_files_directory.is_dir() {
        let error_info = format!("Directory does not exist or it's not a valid path: {:?}", index_path_str.clone());
        ERROR!("{}", error_info);
        return Err(error_info);
    }

    // Load tantivy index with given directory.
    let mut index = match Index::open_in_dir(index_files_directory) {
        Ok(idx) => idx,
        Err(e) => {
            let error_info = format!("Failed to load tantivy index with given directory: {}, exception: {}", index_path_str.clone(), e);
            ERROR!("{}", error_info);
            return Err(error_info);
        }
    };

    // Load custom index settings.
    let custom_index_setting = match load_custom_index_setting(index_files_directory) {
        Ok(setting) => setting,
        Err(e) => {
            let error_info = format!("Error loading custom index settings: {}", e);
            ERROR!("{}", error_info);
            return Err(error_info);
        }
    };

    // Register tokenizer based on the loaded settings.
    let (tokenizer_type, text_analyzer) = match get_custom_tokenizer(&custom_index_setting.tokenizer) {
        Ok((tokenizer_type, text_analyzer)) => (tokenizer_type, text_analyzer),
        Err(e) => {
            let error_info = format!("Failed to initialize tokenizer when loading index: {}", e);
            ERROR!("{}", error_info);
            return Err(error_info);
        }
    };

    if let Err(e) = register_tokenizer_to_index(
        &mut index,
        tokenizer_type.clone(),
        text_analyzer,
    ) {
        let error_info = format!("Failed to register tokenizer when loading index: {:?}, exception: {}", tokenizer_type.name(), e);
        ERROR!("{}", error_info);
        return Err(error_info);
    }

    // Set the multithreaded executor for search.
    if let Err(e) = index.set_default_multithread_executor() {
        let error_info = format!("Failed to set default multithread executor: {}", e);
        ERROR!("{}", error_info);
        return Err(error_info);
    }

    // Create a reader for the index with an appropriate reload policy.
    // OnCommit: reload when commit; Manual: developer need call IndexReader::reload() to reload.
    let reader = match index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()
    {
        Ok(r) => r,
        Err(e) => {
            let error_info = format!("Failed to create tantivy reader: {}", e);
            ERROR!("{}", error_info);
            return Err(error_info);
        }
    };

    // Save IndexR to cache.
    let indexr = IndexR {index, reader, path: index_path_str.clone()};

    if let Err(e) = set_index_r(index_path_str.clone(), Arc::new(indexr)) {
        ERROR!("{}", e);
        return Err(e);
    }

    Ok(true)
}


/// Frees the index reader.
///
/// Arguments:
/// - `index_path`: The directory path for building the index.
///
/// Returns:
/// - A bool value represent operation success.
pub fn tantivy_reader_free(index_path: &CxxString) -> Result<bool, String> {
    // Parse parameter.
    let index_path_str = match index_path.to_str() {
        Ok(content) => content.to_string(),
        Err(e) => {
            return Err(format!("Can't parse parameter index_path: {}, exception: {}", index_path, e.to_string()));
        }
    };

    // remove index reader from CACHE
    remove_index_r(index_path_str)?;

    // success remove.
    Ok(true)
}


/// Determines if a query string appears within a specified row ID range.
///
/// Arguments:
/// - `index_path`: The directory path for building the index.
/// - `query`: Query string.
/// - `lrange`: The left (inclusive) boundary of the row ID range.
/// - `rrange`: The right (inclusive) boundary of the row ID range.
/// - `use_regrex`: Whether use regex searcher.
///
/// Returns:
/// - A bool value represent whether granule hitted.
pub fn tantivy_search_in_rowid_range(
    index_path: &CxxString,
    query: &CxxString,
    lrange: u64,
    rrange: u64,
    use_regex: bool,
) -> Result<bool, String> {
    // Parse parameter.
    let index_path_str = match index_path.to_str() {
        Ok(content) => content.to_string(),
        Err(e) => {
            return Err(format!("Can't parse parameter index_path: {}, exception: {}", index_path, e.to_string()));
        }
    };
    let query_str = match query.to_str() {
        Ok(content) => content.to_string(),
        Err(e) => {
            return Err(format!("Can't parse parameter index_path: {}, exception: {}", query, e.to_string()));
        }
    };
    // get index reader from CACHE
    let index_r = match get_index_r(index_path_str.clone()) {
        Ok(content) => content,
        Err(e) => {
            ERROR!("{}", e);
            return Err(e);
        }
    };

    match perform_search(
        &index_r,
        &query_str,
        lrange,
        rrange,
        use_regex,
    ) {
        Ok(row_id_range) => Ok(!row_id_range.is_empty()),
        Err(e) => {
            let error_info = format!("Error in search: {}", e);
            ERROR!("{}", error_info);
            return Err(error_info);
        }
    }
}


/// Counts the occurrences of a query string within a specified row ID range.
///
/// Arguments:
/// - `index_path`: The directory path for building the index.
/// - `query`: Query string.
/// - `lrange`: The left (inclusive) boundary of the row ID range.
/// - `rrange`: The right (inclusive) boundary of the row ID range.
/// - `use_regrex`: Whether use regex searcher.
///
/// Returns:
/// - The count of occurrences of the query string within the row ID range.
pub fn tantivy_count_in_rowid_range(
    index_path: &CxxString,
    query: &CxxString,
    lrange: u64,
    rrange: u64,
    use_regex: bool,
) -> Result<u64, String> {
    // Parse parameter.
    let index_path_str = match index_path.to_str() {
        Ok(content) => content.to_string(),
        Err(e) => {
            return Err(format!("Can't parse parameter index_path: {}, exception: {}", index_path, e.to_string()));
        }
    };
    let query_str = match query.to_str() {
        Ok(content) => content.to_string(),
        Err(e) => {
            return Err(format!("Can't parse parameter index_path: {}, exception: {}", query, e.to_string()));
        }
    };
    // get index reader from CACHE
    let index_r = match get_index_r(index_path_str.clone()) {
        Ok(content) => content,
        Err(e) => {
            ERROR!("{}", e);
            return Err(e);
        }
    };

    match perform_search(
        &index_r,
        &query_str,
        lrange,
        rrange,
        use_regex,
    ) {
        Ok(row_id_range) => Ok(row_id_range.len() as u64),
        Err(e) => {
            let error_info = format!("Error in search: {}", e);
            ERROR!("{}", error_info);
            return Err(error_info);
        }
    }
}
