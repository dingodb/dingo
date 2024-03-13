use crate::common::errors::IndexSearcherError;
use crate::common::errors::TantivySearchError;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::bridge::index_reader_bridge;
use crate::tokenizer::vo::tokenizers_vo::TokenizerConfig;
use crate::utils::index_utils::IndexUtils;
use crate::RowIdWithScore;
use crate::DEBUG;
use crate::FFI_INDEX_SEARCHER_CACHE;
use crate::{common::constants::LOG_CALLBACK, ERROR, INFO};
use roaring::RoaringBitmap;
use tantivy::schema::Field;
use tantivy::TantivyError;
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

    DEBUG!(function:"load_index", "parameter DTO is {:?}", index_parameter_dto);

    // Parse tokenizer map from local index parameter DTO.
    let col_tokenizer_map: HashMap<String, TokenizerConfig> =
        ToeknizerUtils::parse_tokenizer_json_to_config_map(
            &index_parameter_dto.tokenizers_json_parameter,
        )
        .map_err(|e| {
            ERROR!("{}", e.to_string());
            TantivySearchError::TokenizerUtilsError(e)
        })?;

    DEBUG!(function:"load_index", "col_tokenizer_map len is {:?}", col_tokenizer_map.len());

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
    let index_reader_bridge = IndexReaderBridge {
        index,
        reader,
        path: index_path.trim_end_matches('/').to_string(),
    };

    if let Err(e) =
        FFI_INDEX_SEARCHER_CACHE.set_index_reader_bridge(index_path.to_string(), Arc::new(index_reader_bridge))
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
    column_names: &Vec<String>,
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

    // get index_reader_bridge from CACHE
    let index_reader_bridge = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()).map_err(|e|{
        ERROR!(function:"search_in_rowid_range", "{}", e);
        TantivySearchError::InternalError(e)
    })?;

    match FFiIndexSearcherUtils::perform_search_with_range(
        &index_reader_bridge, &column_names, query, lrange, rrange, use_regex,
    ) {
        Ok(row_id_range) => Ok(!row_id_range.is_empty()),
        Err(e) => {
            ERROR!(function:"search_in_rowid_range", "{}", e);
            return Err(TantivySearchError::IndexSearcherError(e));
        }
    }
}

pub fn count_in_rowid_range(
    index_path: &str,
    column_names: &Vec<String>,
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
    // get index_reader_bridge from CACHE
    let index_reader_bridge = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()).map_err(|e|{
        ERROR!(function:"count_in_rowid_range", "{}", e);
        TantivySearchError::InternalError(e)
    })?;

    match FFiIndexSearcherUtils::perform_search_with_range(
        &index_reader_bridge, column_names, query, lrange, rrange, use_regex,
    ) {
        Ok(row_id_range) => Ok(row_id_range.len() as u64),
        Err(e) => {
            ERROR!(function:"count_in_rowid_range", "{}", e);
            return Err(TantivySearchError::IndexSearcherError(e));
        }
    }
}

pub fn bm25_search_with_filter(
    index_path: &str,
    column_names: &Vec<String>,
    query: &str,
    u8_bitmap: &Vec<u8>,
    top_k: u32,
    need_text: bool,
) -> Result<Vec<RowIdWithScore>, TantivySearchError> {
    let row_ids: Vec<u32> = ConvertUtils::u8_bitmap_to_row_ids(&u8_bitmap);

    // get index_reader_bridge from CACHE
    let index_reader_bridge = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()).map_err(|e|{
        ERROR!(function:"bm25_search_with_filter", "{}", e);
        TantivySearchError::InternalError(e)
    })?;

    let schema = index_reader_bridge.reader.searcher().index().schema();

    let fields: Result<Vec<Field>, TantivyError> = column_names.iter().map(|column_name|{
        schema.get_field(column_name.as_str())
    }).collect();

    let mut fields: Vec<Field> = fields.map_err(|e|{
        let error = TantivySearchError::TantivyError(e);
        ERROR!(function:"bm25_search_with_filter", "{}", error);
        error
    })?;

    if fields.len()==0 {
        fields = schema.fields().filter(|(field,_)|{
            schema.get_field_name(*field)!="row_id"
        }).map(|(field, _)| field).collect();
    }

    if fields.len()==0 {
        let error = IndexSearcherError::EmptyFieldsError;
        ERROR!(function:"bm25_search_with_filter", "{}", error);
        return Err(TantivySearchError::IndexSearcherError(error));
    }

    if need_text {
        for field in fields.clone()  {
            if !schema.get_field_entry(field).is_stored() {
                let error = format!("Can't search with origin text, index doesn't store it");
                ERROR!(function:"bm25_search_with_filter", "{}", error);
                return Err(TantivySearchError::InternalError(error));
            }
        }        
    }

    let searcher = index_reader_bridge.reader.searcher();

    let mut top_docs_collector = TopDocsWithFilter::with_limit(top_k as usize)
        .with_searcher(searcher.clone())
        .with_text_fields(fields.clone())
        .with_stored_text(need_text);

    let mut alive_bitmap = RoaringBitmap::new();
    alive_bitmap.extend(row_ids);

    // if u8_bitmap is empty, we regards that don't use alive_bitmap.
    if u8_bitmap.len() != 0 {
        top_docs_collector = top_docs_collector.with_alive(Arc::new(alive_bitmap));
    }

    let query_parser = QueryParser::for_index(index_reader_bridge.reader.searcher().index(), fields);
    let text_query = query_parser.parse_query(query).map_err(|e|{
        let error = IndexSearcherError::QueryParserError(e.to_string());
        ERROR!(function:"bm25_search_with_filter", "{}", error);
        TantivySearchError::IndexSearcherError(error)
    })?;

    let searched_result = searcher.search(&text_query, &top_docs_collector).map_err(|e|{
        ERROR!(function:"bm25_search_with_filter", "Error when execute query:{}. {}", query, e);
        TantivySearchError::TantivyError(e)
    })?;

    Ok(searched_result)
}

pub fn bm25_search(
    index_path: &str,
    column_names: &Vec<String>,
    query: &str,
    top_k: u32,
    need_text: bool,
) -> Result<Vec<RowIdWithScore>, TantivySearchError> {
    let u8bitmap: Vec<u8> = vec![];
    bm25_search_with_filter(index_path, column_names, query, &u8bitmap, top_k, need_text)
}

pub fn search_bitmap_results(
    index_path: &str,
    column_names: &Vec<String>,
    query: &str,
    use_regex: bool,
) -> Result<Vec<u8>, TantivySearchError> {
    // get index_reader_bridge from CACHE
    let index_reader_bridge = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()).map_err(|e|{
        ERROR!(function:"search_bitmap_results", "{}", e);
        TantivySearchError::InternalError(e)
    })?;

    let row_ids_bitmap = FFiIndexSearcherUtils::perform_search(&index_reader_bridge, &column_names, query, use_regex).map_err(|e|{
        ERROR!(function:"search_bitmap_results", "{}", e);
        TantivySearchError::IndexSearcherError(e)
    })?;

    let row_ids_number: Vec<u32> = row_ids_bitmap.iter().collect();
    let u8_bitmap: Vec<u8> = ConvertUtils::row_ids_to_u8_bitmap(&row_ids_number);
    Ok(u8_bitmap)
}

pub fn indexed_doc_counts(index_path: &str) -> Result<u64, TantivySearchError> {
    // get index_reader_bridge from CACHE
    let index_reader_bridge = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()).map_err(|e|{
        ERROR!(function:"search_bitmap_results", "{}", e);
        TantivySearchError::InternalError(e)
    })?;

    let num_docs = index_reader_bridge.reader.searcher().num_docs();
    Ok(num_docs)
}
