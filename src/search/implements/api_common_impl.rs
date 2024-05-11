use crate::common::errors::TantivySearchError;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::tokenizer::dto::index_parameter_dto::IndexParameterDTO;
use crate::tokenizer::vo::tokenizers_vo::TokenizerConfig;
use crate::utils::index_utils::IndexUtils;
use crate::DEBUG;
use crate::FFI_INDEX_SEARCHER_CACHE;
use crate::{common::constants::LOG_CALLBACK, ERROR};

use std::{path::Path, sync::Arc};

use crate::search::bridge::index_reader_bridge::IndexReaderBridge;
use crate::tokenizer::tokenizer_utils::TokenizerUtils;
use std::collections::HashMap;
use tantivy::Directory;
use tantivy::IndexReader;
use tantivy::{Index, ReloadPolicy};

pub fn get_index_json_parameter(index_path: &str) -> Result<String, TantivySearchError> {
    // Verify index files directory.
    let index_files_directory = Path::new(index_path);
    if !index_files_directory.exists() || !index_files_directory.is_dir() {
        let error_info: String = format!("index_path not exists: {:?}", index_path);
        let error: TantivySearchError = TantivySearchError::IndexNotExists(error_info);
        ERROR!(function:"get_index_json_parameter", "{}", error.to_string());
        return Err(error);
    }

    // Load index parameter DTO from local index files.
    let index_parameter_dto: IndexParameterDTO = IndexUtils::load_custom_index_setting(
        index_files_directory,
    )
    .map_err(|e: crate::common::errors::IndexUtilsError| {
        ERROR!(function:"get_index_json_parameter", "{}", e);
        TantivySearchError::IndexUtilsError(e)
    })?;

    DEBUG!(function:"get_index_json_parameter", "parameter DTO is {:?}", index_parameter_dto);

    Ok(index_parameter_dto.tokenizers_json_parameter.clone())
}

pub fn get_index_meta_json(index_path: &str) -> Result<String, TantivySearchError> {
    // Verify index files directory.
    let index_files_directory = Path::new(index_path);
    if !index_files_directory.exists() || !index_files_directory.is_dir() {
        let error_info: String = format!("index_path not exists: {:?}", index_path);
        let error: TantivySearchError = TantivySearchError::IndexNotExists(error_info);
        ERROR!(function:"get_index_meta_json", "{}", error.to_string());
        return Err(error);
    }

    // Load tantivy index with given directory.
    let index: Index = Index::open_in_dir(index_files_directory).map_err(|e| {
        let error: TantivySearchError = TantivySearchError::TantivyError(e);
        ERROR!(function:"load_index_reader", "{}", error.to_string());
        error
    })?;

    let meta_json_path = Path::new("meta.json");

    let meta_data = match index.directory().atomic_read(&meta_json_path) {
        Ok(data) => data,
        Err(e) => {
            ERROR!(function:"get_index_meta_json", "Failed to read meta.json: {}", e);
            let error_info: String = format!(
                "read path for json failed: {:?}, Failed to read meta.json, error_msg: {}",
                meta_json_path, e
            );
            let error: TantivySearchError = TantivySearchError::InternalError(error_info);
            return Err(error);
        }
    };

    let meta_string = match String::from_utf8(meta_data) {
        Ok(meta_string) => meta_string,
        Err(e) => {
            ERROR!(function:"get_index_meta_json", "Meta data is not valid utf8: {}", e);
            let error_info: String = format!(
                "Meta file does not contain valid utf8 file. {:?}, error_msg: {}",
                meta_json_path, e
            );
            let error: TantivySearchError = TantivySearchError::InternalError(error_info);
            return Err(error);
        }
    };

    DEBUG!(function:"get_index_meta_json", "json is {:?}", &meta_string);

    Ok(meta_string)
}

pub fn load_index_reader(index_path: &str) -> Result<bool, TantivySearchError> {
    // Verify index files directory.
    let index_files_directory = Path::new(index_path);
    if !index_files_directory.exists() || !index_files_directory.is_dir() {
        let error_info: String = format!("index_path not exists: {:?}", index_path);
        let error: TantivySearchError = TantivySearchError::IndexNotExists(error_info);
        ERROR!(function:"load_index_reader", "{}", error.to_string());
        return Err(error);
    }

    // Load tantivy index with given directory.
    let mut index: Index = Index::open_in_dir(index_files_directory).map_err(|e| {
        let error: TantivySearchError = TantivySearchError::TantivyError(e);
        ERROR!(function:"load_index_reader", "{}", error.to_string());
        error
    })?;

    // Load index parameter DTO from local index files.
    let index_parameter_dto: IndexParameterDTO = IndexUtils::load_custom_index_setting(
        index_files_directory,
    )
    .map_err(|e: crate::common::errors::IndexUtilsError| {
        ERROR!(function:"load_index_reader", "{}", e);
        TantivySearchError::IndexUtilsError(e)
    })?;

    DEBUG!(function:"load_index_reader", "parameter DTO is {:?}", index_parameter_dto);

    // Parse tokenizer map from local index parameter DTO.
    let col_tokenizer_map: HashMap<String, TokenizerConfig> =
        TokenizerUtils::parse_tokenizer_json_to_config_map(
            &index_parameter_dto.tokenizers_json_parameter,
        )
        .map_err(|e| {
            ERROR!(function:"load_index_reader", "{}", e);
            TantivySearchError::TokenizerUtilsError(e)
        })?;

    // Register tokenizer config into `index`.
    for (column_name, tokenizer_config) in col_tokenizer_map.iter() {
        TokenizerUtils::register_tokenizer_to_index(
            &mut index,
            tokenizer_config.tokenizer_type.clone(),
            &column_name,
            tokenizer_config.text_analyzer.clone(),
        )
        .map_err(|e| {
            ERROR!(function:"load_index_reader", "{}", e);
            TantivySearchError::TokenizerUtilsError(e)
        })?;
    }

    #[cfg(feature = "use-shared-search-pool")]
    {
        // Set the multithreaded executor for search.
        match FFI_INDEX_SEARCHER_CACHE.get_shared_multithread_executor(2) {
            Ok(shared_thread_pool) => {
                index.set_executor(shared_thread_pool.as_ref().clone());
                DEBUG!(function:"load_index_reader", "Using shared multithread with index_path: [{}]", index_path);
            }
            Err(e) => {
                ERROR!(function:"load_index_reader", "Failed to use shared multithread executor, due to: {}", e);
                index.set_default_multithread_executor().map_err(|e| {
                    ERROR!(function:"load_index_reader", "Failed fall back to default multithread executor, due to: {}", e);
                    TantivySearchError::TantivyError(e)
                })?;
            }
        }
    }
    #[cfg(not(feature = "use-shared-search-pool"))]
    {
        index.set_default_multithread_executor().map_err(|e| {
            ERROR!(function:"load_index_reader", "Failed to set default multithread executor, due to: {}", e);
            TantivySearchError::TantivyError(e)
        })?;
    }

    // Create a reader for the index with an appropriate reload policy.
    // OnCommit: reload when commit; Manual: developer need call IndexReader::reload() to reload.
    let reader: IndexReader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()
        .map_err(|e| {
            ERROR!(function:"load_index_reader", "Failed to create tantivy index reader: {}", e);
            TantivySearchError::TantivyError(e)
        })?;

    // Save IndexReaderBridge to cache.
    let index_reader_bridge: IndexReaderBridge = IndexReaderBridge {
        index,
        reader,
        path: index_path.trim_end_matches('/').to_string(),
    };

    FFI_INDEX_SEARCHER_CACHE
        .set_index_reader_bridge(index_path.to_string(), Arc::new(index_reader_bridge))
        .map_err(|e| {
            ERROR!(function:"load_index_reader", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    Ok(true)
}

pub fn free_index_reader(index_path: &str) -> Result<bool, TantivySearchError> {
    // remove bitmap cache
    // #[cfg(feature = "use-flurry-cache")]
    // {
    //     let all_keys = CACHE_FOR_SKIP_INDEX.all_keys();
    //     let keys_need_remove: Vec<_> = all_keys
    //         .into_iter()
    //         .filter(|(_, _, ref element, _)| element == &index_path)
    //         .collect();
    //     CACHE_FOR_SKIP_INDEX.remove_keys(keys_need_remove);
    // }

    // remove index reader from Reader Cache
    if let Err(_) = FFI_INDEX_SEARCHER_CACHE.remove_index_reader_bridge(index_path.to_string()) {
        return Ok(false);
    }

    // success remove.
    Ok(true)
}

pub fn get_indexed_doc_counts(index_path: &str) -> Result<u64, TantivySearchError> {
    // get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"get_indexed_doc_counts", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    let num_docs: u64 = index_reader_bridge.reader.searcher().num_docs();
    Ok(num_docs)
}
