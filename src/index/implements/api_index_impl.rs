use std::collections::HashMap;
use std::sync::Mutex;
use std::{path::Path, sync::Arc};

use tantivy::schema::IndexRecordOption;
use tantivy::schema::TextFieldIndexing;
use tantivy::schema::TextOptions;
use tantivy::schema::FAST;
use tantivy::schema::{Schema, TEXT};
use tantivy::schema::{INDEXED, STORED};

use crate::common::errors::TantivySearchError;
use crate::index::bridge::index_writer_bridge::IndexWriterBridge;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::implements::api_common_impl::free_index_reader;
use crate::tokenizer::dto::index_parameter_dto::IndexParameterDTO;
use crate::tokenizer::tokenizer_utils::TokenizerUtils;
use crate::tokenizer::vo::tokenizer_types::TokenizerType;
use crate::tokenizer::vo::tokenizers_vo::TokenizerConfig;
use crate::utils::index_utils::IndexUtils;
use crate::{common::constants::LOG_CALLBACK, DEBUG, ERROR, INFO, WARNING};
use crate::{FFI_INDEX_SEARCHER_CACHE, FFI_INDEX_WRITER_CACHE};

use tantivy::{Index, TantivyDocument, Term};

pub fn create_index_with_parameter(
    index_path: &str,
    column_names: &Vec<String>,
    index_json_parameter: &str,
) -> Result<bool, TantivySearchError> {
    // If the `index_path` already exists, it will be recreated,
    // it's necessary to free any `index_reader` associated with this directory.
    free_index_reader(index_path).map_err(|e| {
        ERROR!("{}", e);
        e
    })?;

    // If the `index_path` already exists, it will be recreated,
    // it's necessary to free any `index_writer` associated with this directory.
    free_index_writer(index_path).map_err(|e| {
        ERROR!("{}", e);
        e
    })?;

    // Initialize the index directory, it will store tantivy index files.
    let index_files_directory: &Path = Path::new(index_path);
    IndexUtils::initialize_index_directory(index_files_directory)?;

    // Save custom index json parameter DTO to index directory.
    let index_parameter_dto = IndexParameterDTO {
        tokenizers_json_parameter: index_json_parameter.to_string(),
    };

    DEBUG!(function:"create_index_with_parameter", "parameter DTO:{:?}", index_parameter_dto);

    IndexUtils::save_custom_index_setting(index_files_directory, &index_parameter_dto)?;

    // Parse tokenizer map from local index parameter DTO.
    let col_tokenizer_map: HashMap<String, TokenizerConfig> =
        TokenizerUtils::parse_tokenizer_json_to_config_map(
            &index_parameter_dto.tokenizers_json_parameter,
        )
        .map_err(|e| {
            ERROR!("{}", e.to_string());
            TantivySearchError::TokenizerUtilsError(e)
        })?;

    // Construct the schema for the index.
    let mut schema_builder = Schema::builder();
    schema_builder.add_u64_field("row_id", FAST | INDEXED);

    for column_name in column_names {
        if let Some(tokenizer_config) = col_tokenizer_map.get(column_name) {
            if !tokenizer_config.is_text_field {
                match tokenizer_config.tokenizer_type {
                    TokenizerType::I64(_) => {
                        if tokenizer_config.doc_store && tokenizer_config.doc_index {
                            schema_builder.add_i64_field(&column_name, STORED | INDEXED);
                        } else {
                            if !tokenizer_config.doc_store && tokenizer_config.doc_index {
                                schema_builder.add_i64_field(&column_name, INDEXED);
                            } else {
                                schema_builder.add_i64_field(&column_name, STORED);
                            }
                        }
                        INFO!(function:"create_index_with_parameter", "column_name:{}, field_options name: {}", column_name, "I64");
                        continue;
                    }
                    TokenizerType::F64(_) => {
                        if tokenizer_config.doc_store && tokenizer_config.doc_index {
                            schema_builder.add_f64_field(&column_name, STORED | INDEXED);
                        } else {
                            if !tokenizer_config.doc_store && tokenizer_config.doc_index {
                                schema_builder.add_f64_field(&column_name, INDEXED);
                            } else {
                                schema_builder.add_f64_field(&column_name, STORED);
                            }
                        }
                        INFO!(function:"create_index_with_parameter", "column_name:{}, field_options name: {}", column_name, "F64");
                        continue;
                    }
                    _ => {
                        WARNING!(function:"create_index_with_parameter", "column_name:{}, tokenizer_type:{}, is_text_field:{}",
                            column_name, tokenizer_config.tokenizer_type.name(), tokenizer_config.is_text_field);
                        continue;
                    }
                }
            }

            let tokenizer_name =
                format!("{}_{}", column_name, tokenizer_config.tokenizer_type.name());
            let mut text_options = TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(&tokenizer_name)
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            );

            if tokenizer_config.doc_store {
                text_options = text_options.set_stored();
            }

            INFO!(function:"create_index_with_parameter", "column_name:{}, field_options name: {}", column_name, tokenizer_name);
            schema_builder.add_text_field(&column_name, text_options);
        } else {
            WARNING!(function:"create_index_with_parameter with no_json_schema", "column_name:{}, field_options name: {}", column_name, "TEXT");
            schema_builder.add_text_field(&column_name, TEXT);
        }
    }

    let schema = schema_builder.build();

    INFO!(function:"create_index_with_parameter",
        "index_path:{}, index_json_parameter:{}, col_tokenizer_map size:{}",
        index_path,
        index_json_parameter,
        col_tokenizer_map.len()
    );

    // Create the index in the specified directory.
    let mut index = Index::create_in_dir(index_files_directory, schema).map_err(|e| {
        let error_info = format!(
            "Failed to create index in directory:{}; exception:{}",
            index_path,
            e.to_string()
        );
        ERROR!(function:"create_index_with_parameter", "{}", error_info);
        TantivySearchError::TantivyError(e)
    })?;

    // Register the tokenizer with the index.
    for (col_name, tokenizer_config) in col_tokenizer_map.iter() {
        TokenizerUtils::register_tokenizer_to_index(
            &mut index,
            tokenizer_config.tokenizer_type.clone(),
            &col_name,
            tokenizer_config.text_analyzer.clone(),
        )
        .map_err(|e| {
            ERROR!(function:"create_index_with_parameter", "{}", e.to_string());
            TantivySearchError::TokenizerUtilsError(e)
        })?;
    }

    // Create the writer with a specified buffer size (e.g., 64 MB).
    let writer = index
        .writer_with_num_threads(2, 1024 * 1024 * 64)
        .map_err(|e| {
            let error_info = format!("Failed to create tantivy writer: {}", e);
            ERROR!(function:"create_index_with_parameter", "{}", error_info);
            TantivySearchError::TantivyError(e)
        })?;

    // Configure and set the merge policy.
    let mut merge_policy = tantivy::merge_policy::LogMergePolicy::default();
    merge_policy.set_min_num_segments(5);
    writer.set_merge_policy(Box::new(merge_policy));

    // Save index_writer_bridge to cache.
    let index_writer_bridge: IndexWriterBridge = IndexWriterBridge {
        index,
        path: index_path.trim_end_matches('/').to_string(),
        writer: Mutex::new(Some(writer)),
    };

    FFI_INDEX_WRITER_CACHE
        .set_index_writer_bridge(index_path.to_string(), Arc::new(index_writer_bridge))
        .map_err(|e| {
            ERROR!(function:"create_index_with_parameter", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    Ok(true)
}

pub fn create_index(
    index_path: &str,
    column_names: &Vec<String>,
) -> Result<bool, TantivySearchError> {
    create_index_with_parameter(index_path, column_names, "{}")
}

pub fn index_multi_column_docs(
    index_path: &str,
    row_id: u64,
    column_names: &Vec<String>,
    column_docs: &Vec<String>,
) -> Result<bool, TantivySearchError> {
    // Get index writer from CACHE
    let index_writer_bridge = FFI_INDEX_WRITER_CACHE
        .get_index_writer_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function: "index_multi_column_docs", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    // Get schema from index writer.
    let schema = index_writer_bridge.index.schema();
    let row_id_field = schema.get_field("row_id").map_err(|e| {
        ERROR!(function: "index_multi_column_docs", "Failed to get row_id field: {}", e.to_string());
        TantivySearchError::TantivyError(e)
    })?;

    let mut doc = TantivyDocument::default();
    doc.add_u64(row_id_field, row_id);

    let mut column_idx = 0;
    for column_name in column_names {
        let column_field = schema.get_field(column_name).map_err(|e| {
            ERROR!(function: "index_multi_column_docs", "Failed to get {} field in schema: {}", column_name, e.to_string());
            TantivySearchError::TantivyError(e)
        })?;
        doc.add_text(column_field, column_docs[column_idx].clone());
        column_idx += 1;
    }

    match index_writer_bridge.add_document(doc) {
        Ok(_) => Ok(true),
        Err(e) => {
            let error_info = format!("Failed to index doc:{}", e);
            ERROR!(function: "index_multi_column_docs", "{}", error_info);
            Err(TantivySearchError::InternalError(e))
        }
    }
}

pub fn index_multi_type_column_docs(
    index_path: &str,
    row_id: u64,
    text_column_names: &Vec<String>,
    text_column_docs: &Vec<String>,
    i64_column_names: &Vec<String>,
    i64_column_docs: &Vec<i64>,
    f64_column_names: &Vec<String>,
    f64_column_docs: &Vec<f64>,
) -> Result<bool, TantivySearchError> {
    // Get index writer from CACHE
    let index_writer_bridge = FFI_INDEX_WRITER_CACHE
        .get_index_writer_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function: "index_multi_column_docs", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    // Get schema from index writer.
    let schema = index_writer_bridge.index.schema();
    let row_id_field = schema.get_field("row_id").map_err(|e| {
        ERROR!(function: "index_multi_column_docs", "Failed to get row_id field: {}", e.to_string());
        TantivySearchError::TantivyError(e)
    })?;

    let mut doc = TantivyDocument::default();
    doc.add_u64(row_id_field, row_id);

    let mut column_idx = 0;
    for column_name in text_column_names {
        let column_field = schema.get_field(column_name).map_err(|e| {
            ERROR!(function: "index_multi_column_docs", "Failed to get {} field in schema: {}", column_name, e.to_string());
            TantivySearchError::TantivyError(e)
        })?;
        doc.add_text(column_field, text_column_docs[column_idx].clone());
        column_idx += 1;
    }

    column_idx = 0;
    for column_name in i64_column_names {
        let column_field = schema.get_field(column_name).map_err(|e| {
            ERROR!(function: "index_multi_column_docs", "Failed to get {} field in schema: {}", column_name, e.to_string());
            TantivySearchError::TantivyError(e)
        })?;
        doc.add_i64(column_field, i64_column_docs[column_idx]);
        column_idx += 1;
    }

    column_idx = 0;
    for column_name in f64_column_names {
        let column_field = schema.get_field(column_name).map_err(|e| {
            ERROR!(function: "index_multi_column_docs", "Failed to get {} field in schema: {}", column_name, e.to_string());
            TantivySearchError::TantivyError(e)
        })?;
        doc.add_f64(column_field, f64_column_docs[column_idx]);
        column_idx += 1;
    }

    match index_writer_bridge.add_document(doc) {
        Ok(_) => Ok(true),
        Err(e) => {
            let error_info = format!("Failed to index doc:{}", e);
            ERROR!(function: "index_multi_column_docs", "{}", error_info);
            Err(TantivySearchError::InternalError(e))
        }
    }
}

pub fn delete_row_ids(index_path: &str, row_ids: &Vec<u32>) -> Result<bool, TantivySearchError> {
    // Get index writer from CACHE
    let index_writer_bridge =
        match FFI_INDEX_WRITER_CACHE.get_index_writer_bridge(index_path.to_string()) {
            Ok(content) => content,
            Err(e) => {
                ERROR!(function: "delete_row_ids", "{}", e);
                return Err(TantivySearchError::InternalError(e));
            }
        };

    let schema = index_writer_bridge.index.schema();
    let row_id_field = schema.get_field("row_id").map_err(|e| {
        ERROR!(function: "delete_row_ids", "Failed to get row_id field: {}", e.to_string());
        TantivySearchError::TantivyError(e)
    })?;

    let terms = row_ids
        .iter()
        .map(|row_id| Term::from_field_u64(row_id_field, *row_id as u64))
        .collect();

    // Delete row_id terms.
    index_writer_bridge.delete_terms(terms).map_err(|e| {
        ERROR!(function: "delete_row_ids", "{}", e);
        TantivySearchError::InternalError(e)
    })?;
    // After delete_term, need commit index writer.
    index_writer_bridge.commit().map_err(|e| {
        let error_info = format!("Failed to commit index writer: {}", e.to_string());
        ERROR!(function: "delete_row_ids", "{}", error_info);
        TantivySearchError::InternalError(error_info)
    })?;
    // Try reload index reader from CACHE
    let reload_status = match FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
    {
        Ok(current_index_reader) => match current_index_reader.reload() {
            Ok(_) => true,
            Err(e) => {
                ERROR!(function: "delete_row_ids", "Can't reload reader after delete operation: {}", e);
                return Err(TantivySearchError::InternalError(e));
            }
        },
        Err(e) => {
            WARNING!(function: "delete_row_ids", "{}, skip reload it. ", e);
            true
        }
    };
    Ok(reload_status)
}

pub fn commit_index(index_path: &str) -> Result<bool, TantivySearchError> {
    // get index writer bridge from CACHE
    let index_writer_bridge: Arc<IndexWriterBridge> = FFI_INDEX_WRITER_CACHE
        .get_index_writer_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function: "commit_index", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    index_writer_bridge.commit().map_err(|e| {
        let error_info = format!("Failed to commit index writer: {}", e.to_string());
        ERROR!(function: "commit_index", "{}", error_info);
        TantivySearchError::InternalError(e)
    })?;

    // get index writer bridge from CACHE
    match FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()) {
        Ok(index_reader_bridge) => {
            let _ = index_reader_bridge.reader.reload();
        }
        Err(_) => {}
    }

    Ok(true)
}

pub fn free_index_writer(index_path: &str) -> Result<bool, TantivySearchError> {
    // get index writer bridge from CACHE
    let index_writer_bridge: Arc<IndexWriterBridge> =
        match FFI_INDEX_WRITER_CACHE.get_index_writer_bridge(index_path.to_string()) {
            Ok(content) => content,
            Err(e) => {
                DEBUG!(function: "free_index_writer", "Index writer already been removed: {}", e);
                return Ok(false);
            }
        };
    index_writer_bridge.wait_merging_threads().map_err(|e| {
        let error_info = format!("Can't wait merging threads, exception: {}", e);
        ERROR!(function: "free_index_writer", "{}", error_info);
        TantivySearchError::InternalError(error_info)
    })?;

    // Remove index writer from CACHE
    FFI_INDEX_WRITER_CACHE
        .remove_index_writer_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function: "free_index_writer", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    DEBUG!(function: "free_index_writer", "Index writer has been freed:[{}]", index_path);
    Ok(true)
}

pub fn load_index_writer(index_path: &str) -> Result<bool, TantivySearchError> {
    // Verify index files directory.
    let index_files_directory = Path::new(index_path);
    if !index_files_directory.exists() || !index_files_directory.is_dir() {
        let error_info: String = format!("index_path not exists: {:?}", index_path);
        let error: TantivySearchError = TantivySearchError::IndexNotExists(error_info);
        ERROR!(function:"load_index_writer", "{}", error.to_string());
        return Err(error);
    }

    // Load tantivy index with given directory.
    let mut index: Index = Index::open_in_dir(index_files_directory).map_err(|e| {
        let error: TantivySearchError = TantivySearchError::TantivyError(e);
        ERROR!(function:"load_index_writer", "{}", error.to_string());
        error
    })?;

    // Load index parameter DTO from local index files.
    let index_parameter_dto: IndexParameterDTO =
        IndexUtils::load_custom_index_setting(index_files_directory).map_err(|e| {
            ERROR!(function:"load_index_writer", "{}", e);
            TantivySearchError::IndexUtilsError(e)
        })?;

    DEBUG!(function:"load_index_writer", "parameter DTO is {:?}", index_parameter_dto);

    // Parse tokenizer map from local index parameter DTO.
    let col_tokenizer_map: HashMap<String, TokenizerConfig> =
        TokenizerUtils::parse_tokenizer_json_to_config_map(
            &index_parameter_dto.tokenizers_json_parameter,
        )
        .map_err(|e| {
            ERROR!(function:"load_index_writer", "{}", e);
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
            ERROR!(function:"load_index_writer", "{}", e);
            TantivySearchError::TokenizerUtilsError(e)
        })?;
    }

    #[cfg(feature = "use-shared-search-pool")]
    {
        // Set the multithreaded executor for search.
        match FFI_INDEX_SEARCHER_CACHE.get_shared_multithread_executor(2) {
            Ok(shared_thread_pool) => {
                index.set_executor(shared_thread_pool.as_ref().clone());
                DEBUG!(function:"load_index_writer", "Using shared multithread with index_path: [{}]", index_path);
            }
            Err(e) => {
                ERROR!(function:"load_index_writer", "Failed to use shared multithread executor, due to: {}", e);
                index.set_default_multithread_executor().map_err(|e| {
                    ERROR!(function:"load_index_writer", "Failed fall back to default multithread executor, due to: {}", e);
                    TantivySearchError::TantivyError(e)
                })?;
            }
        }
    }
    #[cfg(not(feature = "use-shared-search-pool"))]
    {
        index.set_default_multithread_executor().map_err(|e| {
            ERROR!(function:"load_index_writer", "Failed to set default multithread executor, due to: {}", e);
            TantivySearchError::TantivyError(e)
        })?;
    }

    // Create the writer with a specified buffer size (e.g., 64 MB).
    let writer = index
        .writer_with_num_threads(2, 1024 * 1024 * 64)
        .map_err(|e| {
            let error_info = format!("Failed to create tantivy writer: {}", e);
            ERROR!(function:"load_index_writer", "{}", error_info);
            TantivySearchError::TantivyError(e)
        })?;

    // Configure and set the merge policy.
    let mut merge_policy = tantivy::merge_policy::LogMergePolicy::default();
    merge_policy.set_min_num_segments(5);
    writer.set_merge_policy(Box::new(merge_policy));

    // Save index_writer_bridge to cache.
    let index_writer_bridge: IndexWriterBridge = IndexWriterBridge {
        index,
        path: index_path.trim_end_matches('/').to_string(),
        writer: Mutex::new(Some(writer)),
    };

    FFI_INDEX_WRITER_CACHE
        .set_index_writer_bridge(index_path.to_string(), Arc::new(index_writer_bridge))
        .map_err(|e| {
            ERROR!(function:"load_index_writer", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    Ok(true)
}
