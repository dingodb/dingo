use crate::common::constants::convert_cxx_string;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::tokenizer::parse_and_register::get_custom_tokenizer;
use crate::tokenizer::parse_and_register::register_tokenizer_to_index;
use crate::RowIdWithScore;
use crate::FFI_INDEX_SEARCHER_CACHE;
use crate::{common::constants::LOG_CALLBACK, ERROR, INFO, WARNING};
use cxx::CxxString;
use cxx::CxxVector;
use cxx::UniquePtr;
use roaring::RoaringBitmap;
use std::{path::Path, sync::Arc};
use tantivy::query::QueryParser;

use super::bridge::index_reader_bridge::IndexReaderBridge;
use super::collector::top_dos_with_bitmap_collector::TopDocsWithFilter;
use super::ffi_index_searcher_utils::ConvertUtils;
use super::ffi_index_searcher_utils::FFiIndexSearcherUtils;
use crate::common::constants::CACHE_FOR_SKIP_INDEX;
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
    let index_path_str = convert_cxx_string("tantivy_load_index", "index_path", index_path)?;

    // Verify index files directory.
    let index_files_directory = Path::new(&index_path_str);
    if !index_files_directory.exists() || !index_files_directory.is_dir() {
        let error_info = format!(
            "Directory does not exist or it's not a valid path: {:?}",
            index_path_str.clone()
        );
        ERROR!("{}", error_info);
        return Err(error_info);
    }

    // Load tantivy index with given directory.
    let mut index = match Index::open_in_dir(index_files_directory) {
        Ok(idx) => idx,
        Err(e) => {
            let error_info = format!(
                "Failed to load tantivy index with given directory: {}, exception: {}",
                index_path_str.clone(),
                e
            );
            ERROR!("{}", error_info);
            return Err(error_info);
        }
    };

    // Load custom index settings.
    let custom_index_setting = match IndexUtils::load_custom_index_setting(index_files_directory) {
        Ok(setting) => setting,
        Err(e) => {
            let error_info = format!("Error loading custom index settings: {}", e);
            ERROR!("{}", error_info);
            return Err(error_info);
        }
    };

    // Register tokenizer based on the loaded settings.
    let (tokenizer_type, text_analyzer) =
        match get_custom_tokenizer(&custom_index_setting.tokenizer) {
            Ok((tokenizer_type, text_analyzer)) => (tokenizer_type, text_analyzer),
            Err(e) => {
                let error_info =
                    format!("Failed to initialize tokenizer when loading index: {}", e);
                ERROR!("{}", error_info);
                return Err(error_info);
            }
        };

    if let Err(e) = register_tokenizer_to_index(&mut index, tokenizer_type.clone(), text_analyzer) {
        let error_info = format!(
            "Failed to register tokenizer when loading index: {:?}, exception: {}",
            tokenizer_type.name(),
            e
        );
        ERROR!("{}", error_info);
        return Err(error_info);
    }

    #[cfg(feature = "use-shared-search-pool")]
    {
        // Set the multithreaded executor for search.
        match FFI_INDEX_SEARCHER_CACHE.get_shared_multithread_executor(2) {
            Ok(shared_thread_pool) => {
                index
                    .set_shared_multithread_executor(shared_thread_pool)
                    .map_err(|e| e.to_string())?;
                INFO!(
                    "Using shared multithread with index_path: [{}]",
                    index_path_str.clone()
                );
            }
            Err(e) => {
                WARNING!("Failed to use shared multithread executor, due to: {}", e);
                index.set_default_multithread_executor().map_err(|e| {
                    let error_info = format!("Failed to set default multithread executor: {}", e);
                    ERROR!("{}", error_info);
                    error_info
                })?;
            }
        }
    }
    #[cfg(not(feature = "use-shared-search-pool"))]
    {
        index.set_default_multithread_executor().map_err(|e| {
            let error_info = format!("Failed to set default multithread executor: {}", e);
            ERROR!("{}", error_info);
            error_info
        })?;
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

    // Save IndexReaderBridge to cache.
    let indexr = IndexReaderBridge {
        index,
        reader,
        path: index_path_str.trim_end_matches('/').to_string(),
    };

    if let Err(e) =
        FFI_INDEX_SEARCHER_CACHE.set_index_reader_bridge(index_path_str.clone(), Arc::new(indexr))
    {
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
    let index_path_str = convert_cxx_string("tantivy_reader_free", "index_path", index_path)?;

    // remove bitmap cache
    #[cfg(feature = "use-flurry-cache")]
    {
        let all_keys = CACHE_FOR_SKIP_INDEX.all_keys();
        let keys_need_remove: Vec<_> = all_keys
            .into_iter()
            .filter(|(_, _, ref element, _)| element == &index_path_str)
            .collect();
        CACHE_FOR_SKIP_INDEX.remove_keys(keys_need_remove);
    }

    // remove index reader from Reader Cache
    if let Err(_) = FFI_INDEX_SEARCHER_CACHE.remove_index_reader_bridge(index_path_str.clone()) {
        return Ok(false);
    }

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
/// - `use_regex`: Whether use regex searcher.
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
    let index_path_str =
        convert_cxx_string("tantivy_search_in_rowid_range", "index_path", index_path)?;

    let query_str = convert_cxx_string("tantivy_search_in_rowid_range", "query", query)?;
    if lrange > rrange {
        return Err("lrange should smaller than rrange".to_string());
    }
    // get index reader from CACHE
    let index_r = match FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path_str.clone()) {
        Ok(content) => content,
        Err(e) => {
            ERROR!("{}", e);
            return Err(e);
        }
    };

    match FFiIndexSearcherUtils::perform_search_with_range(
        &index_r, &query_str, lrange, rrange, use_regex,
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
/// - `use_regex`: Whether use regex searcher.
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
    let index_path_str =
        convert_cxx_string("tantivy_count_in_rowid_range", "index_path", index_path)?;

    let query_str = convert_cxx_string("tantivy_count_in_rowid_range", "query", query)?;
    if lrange > rrange {
        return Err("lrange should smaller than rrange".to_string());
    }
    // get index reader from CACHE
    let index_r = match FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path_str.clone()) {
        Ok(content) => content,
        Err(e) => {
            ERROR!("{}", e);
            return Err(e);
        }
    };

    match FFiIndexSearcherUtils::perform_search_with_range(
        &index_r, &query_str, lrange, rrange, use_regex,
    ) {
        Ok(row_id_range) => Ok(row_id_range.len() as u64),
        Err(e) => {
            let error_info = format!("Error in search: {}", e);
            ERROR!("{}", error_info);
            Err(error_info)
        }
    }
}

/// Execute bm25_search with filter row_ids.
///
/// Arguments:
/// - `index_path`: The directory path for building the index.
/// - `query`: Query string.
/// - `u8_bitmap`: A vector<u8> bitmap represent alives row_ids.
/// - `top_k`: Try to search `k` results.
/// - `need_text`: Whether need return origin doc content.
///
/// Returns:
/// - A group of RowIdWithScore Objects.
pub fn tantivy_bm25_search_with_filter(
    index_path: &CxxString,
    query: &CxxString,
    u8_bitmap: &CxxVector<u8>,
    top_k: u32,
    need_text: bool,
) -> Result<Vec<RowIdWithScore>, String> {
    // Parse parameter.
    let index_path_str =
        convert_cxx_string("tantivy_bm25_search_with_filter", "index_path", index_path)?;
    let query_str = convert_cxx_string("tantivy_bm25_search_with_filter", "query", query)?;

    let u8_bitmap: Vec<u8> = u8_bitmap.iter().map(|s| *s).collect();
    let row_ids: Vec<u32> = ConvertUtils::u8_bitmap_to_row_ids(&u8_bitmap);
    // INFO!("alive row_ids is: {:?}", row_ids);
    // get index reader from CACHE
    let index_r = match FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path_str.clone()) {
        Ok(content) => content,
        Err(e) => {
            ERROR!("{}", e);
            return Err(e);
        }
    };

    let schema = index_r.reader.searcher().index().schema();
    let text = match schema.get_field("text") {
        Ok(str) => str,
        Err(_) => {
            ERROR!("Missing text field.");
            return Ok(Vec::new());
        }
    };

    let is_stored = schema.get_field_entry(text).is_stored();
    if !is_stored && need_text {
        let error_info = format!("Can't search with origin text, index doesn't store it");
        ERROR!("{}", error_info);
        return Err(error_info);
    }

    let searcher = index_r.reader.searcher();

    let mut top_docs_collector = TopDocsWithFilter::with_limit(top_k as usize)
        .with_searcher(searcher.clone())
        .with_text_field(text)
        .with_stored_text(need_text);

    let mut alive_bitmap = RoaringBitmap::new();
    alive_bitmap.extend(row_ids);

    // if u8_bitmap is empty, we regards that don't use alive_bitmap.
    if u8_bitmap.len() != 0 {
        top_docs_collector = top_docs_collector.with_alive(Arc::new(alive_bitmap));
    }

    let query_parser = QueryParser::for_index(index_r.reader.searcher().index(), vec![text]);
    let text_query = match query_parser.parse_query(&query_str) {
        Ok(parsed_query) => parsed_query,
        Err(e) => {
            let error_info = format!("Can't parse query: {}, due to: {}", query_str, e);
            ERROR!("{}", error_info);
            return Err(error_info);
        }
    };
    let searched_result = match searcher.search(&text_query, &top_docs_collector) {
        Ok(result) => result,
        Err(e) => {
            let error_info = format!(
                "Can't execute search in `tantivy_search_with_row_id_bitmap`: {}",
                e
            );
            ERROR!("{}", error_info);
            return Err(error_info);
        }
    };

    Ok(searched_result)
}

/// Execute bm25_search.
///
/// Arguments:
/// - `index_path`: The directory path for building the index.
/// - `query`: Query string.
/// - `top_k`: Try to search `k` results.
/// - `need_text`: Whether need return origin doc content.
///
/// Returns:
/// - A group of RowIdWithScore Objects.
pub fn tantivy_bm25_search(
    index_path: &CxxString,
    query: &CxxString,
    top_k: u32,
    need_text: bool,
) -> Result<Vec<RowIdWithScore>, String> {
    let cxx_vector: UniquePtr<CxxVector<u8>> = CxxVector::new();
    let cxx_vector: &CxxVector<u8> = cxx_vector.as_ref().unwrap();
    tantivy_bm25_search_with_filter(index_path, query, cxx_vector, top_k, need_text)
}

/// Execute search with like pattern or not.
///
/// Arguments:
/// - `index_path`: The directory path for building the index.
/// - `query`: Query should be like pattern.
/// - `use_regex`: For like pattern, use_regex should be true.
///
/// Returns:
/// - row_ids u8 bitmap.
pub fn tantivy_search_bitmap_results(
    index_path: &CxxString,
    query: &CxxString,
    use_regex: bool,
) -> Result<Vec<u8>, String> {
    // Parse parameter.
    let index_path_str =
        convert_cxx_string("tantivy_search_bitmap_results", "index_path", index_path)?;
    let query_str = convert_cxx_string("tantivy_search_bitmap_results", "query", query)?;

    // get index reader from CACHE
    let index_r = match FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path_str.clone()) {
        Ok(content) => content,
        Err(e) => {
            ERROR!("{}", e);
            return Err(e);
        }
    };

    let row_ids_bitmap =
        match FFiIndexSearcherUtils::perform_search(&index_r, &query_str, use_regex) {
            Ok(content) => content,
            Err(e) => {
                let error_info = format!("Error in perform_search: {}", e);
                ERROR!("{}", error_info);
                return Err(error_info);
            }
        };
    let row_ids_number: Vec<u32> = row_ids_bitmap.iter().collect();
    let u8_bitmap: Vec<u8> = ConvertUtils::row_ids_to_u8_bitmap(&row_ids_number);
    Ok(u8_bitmap)
}
