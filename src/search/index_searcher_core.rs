use crate::common::errors::TantivySearchError;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::tokenizer::vo::tokenizers_vo::TokenizerConfig;
use crate::utils::index_utils::IndexUtils;
use crate::RowIdWithScore;
use crate::FFI_INDEX_SEARCHER_CACHE;
use crate::{common::constants::LOG_CALLBACK, ERROR, INFO};
use roaring::RoaringBitmap;
use std::{path::Path, sync::Arc};
use tantivy::query::QueryParser;

use super::bridge::index_reader_bridge::IndexReaderBridge;
use super::collector::top_dos_with_bitmap_collector::TopDocsWithFilter;
use super::index_searcher_utils::ConvertUtils;
use super::index_searcher_utils::FFiIndexSearcherUtils;
use crate::common::constants::CACHE_FOR_SKIP_INDEX;
use crate::tokenizer::tokenizer_utils::ToeknizerUtils;
use std::collections::HashMap;
use tantivy::{Index, ReloadPolicy};

pub fn load_index(index_path: &str) -> Result<bool, TantivySearchError> {
    // Verify index files directory.
    let index_files_directory = Path::new(index_path);
    if !index_files_directory.exists() || !index_files_directory.is_dir() {
        let error_info = format!("index_path not exists: {:?}", index_path);
        let error = TantivySearchError::IndexNotExists(error_info);
        ERROR!("{}", error.to_string());
        return Err(error);
    }

    // Load tantivy index with given directory.
    let mut index = match Index::open_in_dir(index_files_directory) {
        Ok(idx) => idx,
        Err(e) => {
            let error = TantivySearchError::TantivyError(e);
            ERROR!("{}", error.to_string());
            return Err(error);
        }
    };

    // Load index parameter DTO from local index files.
    let index_parameter_dto = match IndexUtils::load_custom_index_setting(index_files_directory) {
        Ok(setting) => setting,
        Err(e) => {
            ERROR!("{}", e.to_string());
            return Err(TantivySearchError::IndexUtilsError(e));
        }
    };

    // Parse tokenizer map from local index parameter DTO.
    let col_tokenizer_map: HashMap<String, TokenizerConfig> =
        ToeknizerUtils::parse_tokenizer_json_to_config_map(
            &index_parameter_dto.tokenizers_json_parameter,
        )
        .map_err(|e| {
            ERROR!("{}", e.to_string());
            TantivySearchError::TokenizerUtilsError(e)
        })?;

    // Register tokenizer config into `index`.
    for (column_name, tokenizer_config) in col_tokenizer_map.iter() {
        ToeknizerUtils::register_tokenizer_to_index(
            &mut index,
            tokenizer_config.tokenizer_type.clone(),
            &column_name,
            tokenizer_config.text_analyzer.clone(),
        )
        .map_err(|e| {
            ERROR!("{}", e.to_string());
            TantivySearchError::TokenizerUtilsError(e)
        })?;
    }

    #[cfg(feature = "use-shared-search-pool")]
    {
        // Set the multithreaded executor for search.
        match FFI_INDEX_SEARCHER_CACHE.get_shared_multithread_executor(2) {
            Ok(shared_thread_pool) => {
                index
                    .set_shared_multithread_executor(shared_thread_pool)
                    .map_err(|e| TantivySearchError::TantivyError(e))?;
                INFO!("Using shared multithread with index_path: [{}]", index_path);
            }
            Err(e) => {
                ERROR!("Failed to use shared multithread executor, due to: {}", e);
                index.set_default_multithread_executor().map_err(|e| {
                    let error_info = format!("Failed to set default multithread executor: {}", e);
                    ERROR!("{}", error_info);
                    TantivySearchError::TantivyError(e)
                })?;
            }
        }
    }
    #[cfg(not(feature = "use-shared-search-pool"))]
    {
        index.set_default_multithread_executor().map_err(|e| {
            let error_info = format!("Failed to set default multithread executor: {}", e);
            ERROR!("{}", error_info);
            TantivySearchError::TantivyError(e)
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
            return Err(TantivySearchError::TantivyError(e));
        }
    };

    // Save IndexReaderBridge to cache.
    let indexr = IndexReaderBridge {
        index,
        reader,
        path: index_path.trim_end_matches('/').to_string(),
    };

    if let Err(e) =
        FFI_INDEX_SEARCHER_CACHE.set_index_reader_bridge(index_path.to_string(), Arc::new(indexr))
    {
        ERROR!("{}", e);
        return Err(TantivySearchError::InternalError(e));
    }

    Ok(true)
}

pub fn free_reader(index_path: &str) -> Result<bool, TantivySearchError> {
    // remove bitmap cache
    #[cfg(feature = "use-flurry-cache")]
    {
        let all_keys = CACHE_FOR_SKIP_INDEX.all_keys();
        let keys_need_remove: Vec<_> = all_keys
            .into_iter()
            .filter(|(_, _, ref element, _)| element == &index_path)
            .collect();
        CACHE_FOR_SKIP_INDEX.remove_keys(keys_need_remove);
    }

    // remove index reader from Reader Cache
    if let Err(_) = FFI_INDEX_SEARCHER_CACHE.remove_index_reader_bridge(index_path.to_string()) {
        return Ok(false);
    }

    // success remove.
    Ok(true)
}

pub fn search_in_rowid_range(
    index_path: &str,
    query: &str,
    lrange: u64,
    rrange: u64,
    use_regex: bool,
) -> Result<bool, TantivySearchError> {
    if lrange > rrange {
        return Err(TantivySearchError::InvalidArgument(format!(
            "lrange:{}, rrange:{}",
            lrange, rrange
        )));
    }

    // get index reader from CACHE
    let index_r = match FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()) {
        Ok(content) => content,
        Err(e) => {
            ERROR!("{}", e);
            return Err(TantivySearchError::InternalError(e));
        }
    };

    match FFiIndexSearcherUtils::perform_search_with_range(
        &index_r, query, lrange, rrange, use_regex,
    ) {
        Ok(row_id_range) => Ok(!row_id_range.is_empty()),
        Err(e) => {
            let error_info = format!("Error in search: {}", e);
            ERROR!("{}", error_info);
            return Err(TantivySearchError::InternalError(e));
        }
    }
}

pub fn count_in_rowid_range(
    index_path: &str,
    query: &str,
    lrange: u64,
    rrange: u64,
    use_regex: bool,
) -> Result<u64, TantivySearchError> {
    if lrange > rrange {
        return Err(TantivySearchError::InvalidArgument(format!(
            "lrange:{}, rrange:{}",
            lrange, rrange
        )));
    }
    // get index reader from CACHE
    let index_r = match FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()) {
        Ok(content) => content,
        Err(e) => {
            ERROR!("{}", e);
            return Err(TantivySearchError::InternalError(e));
        }
    };

    match FFiIndexSearcherUtils::perform_search_with_range(
        &index_r, query, lrange, rrange, use_regex,
    ) {
        Ok(row_id_range) => Ok(row_id_range.len() as u64),
        Err(e) => {
            let error_info = format!("Error in search: {}", e);
            ERROR!("{}", error_info);
            return Err(TantivySearchError::InternalError(e));
        }
    }
}

pub fn bm25_search_with_filter(
    index_path: &str,
    query: &str,
    u8_bitmap: &Vec<u8>,
    top_k: u32,
    need_text: bool,
) -> Result<Vec<RowIdWithScore>, TantivySearchError> {
    let row_ids: Vec<u32> = ConvertUtils::u8_bitmap_to_row_ids(&u8_bitmap);

    // INFO!("alive row_ids is: {:?}", row_ids);
    // get index reader from CACHE
    let index_r = match FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()) {
        Ok(content) => content,
        Err(e) => {
            ERROR!("{}", e);
            return Err(TantivySearchError::InternalError(e));
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
        return Err(TantivySearchError::InternalError(error_info));
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
    let text_query = match query_parser.parse_query(query) {
        Ok(parsed_query) => parsed_query,
        Err(e) => {
            let error_info = format!("Can't parse query: {}, due to: {}", query, e);
            ERROR!("{}", error_info);
            return Err(TantivySearchError::InternalError(error_info));
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
            return Err(TantivySearchError::InternalError(error_info));
        }
    };

    Ok(searched_result)
}

pub fn bm25_search(
    index_path: &str,
    query: &str,
    top_k: u32,
    need_text: bool,
) -> Result<Vec<RowIdWithScore>, TantivySearchError> {
    let u8bitmap: Vec<u8> = vec![];
    bm25_search_with_filter(index_path, query, &u8bitmap, top_k, need_text)
}

pub fn search_bitmap_results(
    index_path: &str,
    query: &str,
    use_regex: bool,
) -> Result<Vec<u8>, TantivySearchError> {
    // get index reader from CACHE
    let index_r = match FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()) {
        Ok(content) => content,
        Err(e) => {
            ERROR!("{}", e);
            return Err(TantivySearchError::InternalError(e));
        }
    };

    let row_ids_bitmap = match FFiIndexSearcherUtils::perform_search(&index_r, query, use_regex) {
        Ok(content) => content,
        Err(e) => {
            let error_info = format!("Error in perform_search: {}", e);
            ERROR!("{}", error_info);
            return Err(TantivySearchError::InternalError(e));
        }
    };
    let row_ids_number: Vec<u32> = row_ids_bitmap.iter().collect();
    let u8_bitmap: Vec<u8> = ConvertUtils::row_ids_to_u8_bitmap(&row_ids_number);
    Ok(u8_bitmap)
}

pub fn indexed_doc_counts(index_path: &str) -> Result<u64, TantivySearchError> {
    // get index writer from CACHE
    let index_r = match FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()) {
        Ok(content) => content,
        Err(e) => {
            ERROR!(function: "tantivy_indexed_doc_counts", "Index reader already been removed: {}", e);
            return Ok(0);
        }
    };
    let num_docs = index_r.reader.searcher().num_docs();
    Ok(num_docs)
}
