use crate::common::errors::TantivySearchError;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::bridge::index_reader_bridge::IndexReaderBridge;
use crate::search::implements::strategy::query_strategy::QueryExecutor;
use crate::search::implements::strategy::query_strategy::SingleTermQueryStrategy;
use crate::search::utils::convert_utils::ConvertUtils;
use crate::search::utils::index_searcher_utils::FFiIndexSearcherUtils;
use crate::FFI_INDEX_SEARCHER_CACHE;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use roaring::RoaringBitmap;
use std::sync::Arc;

use super::strategy::query_strategy::ParserQueryStrategy;
use super::strategy::query_strategy::RegexQueryStrategy;
use super::strategy::query_strategy::TermSetQueryStrategy;

/**
 * 对于面向 ClickHouse 的 API，只需要执行单列搜索。
 */



/// 执行 range 范围内单个 Term 查询 
pub fn query_term_with_range(
    index_path: &str,
    column_name: &str,
    term: &str,
    lrange: u64,
    rrange: u64,
) -> Result<bool, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()).map_err(|e|{
        ERROR!(function:"query_term_with_range", "{}", e);
        TantivySearchError::InternalError(e)
    })?;

    // Choose query strategy to construct query executor.
    let term_query: SingleTermQueryStrategy<'_> = SingleTermQueryStrategy {
        column_name,
        term,
    };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&term_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor.execute(&index_reader_bridge.reader.searcher()).map_err(|e|{
        ERROR!(function:"query_term_with_range", "{}", e);
        TantivySearchError::IndexSearcherError(e)
    })?;

    // Intersect query results with range.
    let intersected = FFiIndexSearcherUtils::intersect_with_range(result, lrange, rrange).map_err(|e|{
        ERROR!(function:"query_term_with_range", "{}", e);
        TantivySearchError::IndexSearcherError(e)
    })?;
    Ok(!intersected.is_empty())
}

/// 执行 range 范围内多个 Terms 查询
pub fn query_terms_with_range(
    index_path: &str,
    column_name: &str,
    terms: &Vec<String>,
    lrange: u64,
    rrange: u64,
) -> Result<bool, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()).map_err(|e|{
        ERROR!(function:"query_term_with_range", "{}", e);
        TantivySearchError::InternalError(e)
    })?;

    // Choose query strategy to construct query executor.
    let terms_query: TermSetQueryStrategy<'_> = TermSetQueryStrategy {
        column_name,
        terms,
    };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&terms_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor.execute(&index_reader_bridge.reader.searcher()).map_err(|e|{
        ERROR!(function:"query_terms_with_range", "{}", e);
        TantivySearchError::IndexSearcherError(e)
    })?;

    // Intersect query results with range.
    let intersected = FFiIndexSearcherUtils::intersect_with_range(result, lrange, rrange).map_err(|e|{
        ERROR!(function:"query_terms_with_range", "{}", e);
        TantivySearchError::IndexSearcherError(e)
    })?;
    Ok(!intersected.is_empty())
}

/// 执行 range 范围内句子 sentence 查询
pub fn query_sentence_with_range(
    index_path: &str,
    column_name: &str,
    sentence: &str,
    lrange: u64,
    rrange: u64,
) -> Result<bool, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()).map_err(|e|{
        ERROR!(function:"query_term_with_range", "{}", e);
        TantivySearchError::InternalError(e)
    })?;

    // Choose query strategy to construct query executor.
    let sentence_query: ParserQueryStrategy<'_> = ParserQueryStrategy {
        column_name,
        sentence,
    };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&sentence_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor.execute(&index_reader_bridge.reader.searcher()).map_err(|e|{
        ERROR!(function:"query_sentence_with_range", "{}", e);
        TantivySearchError::IndexSearcherError(e)
    })?;

    // Intersect query results with range.
    let intersected = FFiIndexSearcherUtils::intersect_with_range(result, lrange, rrange).map_err(|e|{
        ERROR!(function:"query_sentence_with_range", "{}", e);
        TantivySearchError::IndexSearcherError(e)
    })?;
    Ok(!intersected.is_empty())
}

/// 执行 range 范围内正则匹配 regex
pub fn regex_term_with_range(
    index_path: &str,
    column_name: &str,
    pattern: &str,
    lrange: u64,
    rrange: u64,
) -> Result<bool, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()).map_err(|e|{
        ERROR!(function:"query_term_with_range", "{}", e);
        TantivySearchError::InternalError(e)
    })?;

    // Choose query strategy to construct query executor.
    let sentence_query: RegexQueryStrategy<'_> = RegexQueryStrategy {
        column_name,
        pattern,
    };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&sentence_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor.execute(&index_reader_bridge.reader.searcher()).map_err(|e|{
        ERROR!(function:"regex_term_with_range", "{}", e);
        TantivySearchError::IndexSearcherError(e)
    })?;

    // Intersect query results with range.
    let intersected = FFiIndexSearcherUtils::intersect_with_range(result, lrange, rrange).map_err(|e|{
        ERROR!(function:"regex_term_with_range", "{}", e);
        TantivySearchError::IndexSearcherError(e)
    })?;
    Ok(!intersected.is_empty())
}

/// 执行单个 Term 查询
pub fn query_term_bitmap(
    index_path: &str,
    column_name: &str,
    term: &str,
) -> Result<Vec<u8>, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()).map_err(|e|{
        ERROR!(function:"query_term_bitmap", "{}", e);
        TantivySearchError::InternalError(e)
    })?;

    // Choose query strategy to construct query executor.
    let sentence_query: SingleTermQueryStrategy<'_> = SingleTermQueryStrategy {
        column_name,
        term,
    };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&sentence_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor.execute(&index_reader_bridge.reader.searcher()).map_err(|e|{
        ERROR!(function:"query_term_bitmap", "{}", e);
        TantivySearchError::IndexSearcherError(e)
    })?;

    // Convert results to u8 bitmap.
    let row_ids_number: Vec<u32> = result.iter().collect();
    let u8_bitmap: Vec<u8> = ConvertUtils::row_ids_to_u8_bitmap(&row_ids_number);

    Ok(u8_bitmap)
}

/// 执行多个 Terms 查询
pub fn query_terms_bitmap(
    index_path: &str,
    column_name: &str,
    terms: &Vec<String>,
) -> Result<Vec<u8>, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()).map_err(|e|{
        ERROR!(function:"query_terms_bitmap", "{}", e);
        TantivySearchError::InternalError(e)
    })?;

    // Choose query strategy to construct query executor.
    let sentence_query: TermSetQueryStrategy<'_> = TermSetQueryStrategy {
        column_name,
        terms,
    };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&sentence_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor.execute(&index_reader_bridge.reader.searcher()).map_err(|e|{
        ERROR!(function:"query_terms_bitmap", "{}", e);
        TantivySearchError::IndexSearcherError(e)
    })?;

    // Convert results to u8 bitmap.
    let row_ids_number: Vec<u32> = result.iter().collect();
    let u8_bitmap: Vec<u8> = ConvertUtils::row_ids_to_u8_bitmap(&row_ids_number);

    Ok(u8_bitmap)
}

/// 执行句子 sentence 查询
pub fn query_sentence_bitmap(
    index_path: &str,
    column_name: &str,
    sentence: &str,
) -> Result<Vec<u8>, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()).map_err(|e|{
        ERROR!(function:"query_sentence_bitmap", "{}", e);
        TantivySearchError::InternalError(e)
    })?;

    // Choose query strategy to construct query executor.
    let sentence_query: ParserQueryStrategy<'_> = ParserQueryStrategy {
        column_name,
        sentence,
    };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&sentence_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor.execute(&index_reader_bridge.reader.searcher()).map_err(|e|{
        ERROR!(function:"query_sentence_bitmap", "{}", e);
        TantivySearchError::IndexSearcherError(e)
    })?;

    // Convert results to u8 bitmap.
    let row_ids_number: Vec<u32> = result.iter().collect();
    let u8_bitmap: Vec<u8> = ConvertUtils::row_ids_to_u8_bitmap(&row_ids_number);

    Ok(u8_bitmap)
}

/// 执行正则匹配 regex
pub fn regex_term_bitmap(
    index_path: &str,
    column_name: &str,
    pattern: &str,
) -> Result<Vec<u8>, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()).map_err(|e|{
        ERROR!(function:"regex_term_bitmap", "{}", e);
        TantivySearchError::InternalError(e)
    })?;

    // Choose query strategy to construct query executor.
    let sentence_query: RegexQueryStrategy<'_> = RegexQueryStrategy {
        column_name,
        pattern,
    };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&sentence_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor.execute(&index_reader_bridge.reader.searcher()).map_err(|e|{
        ERROR!(function:"regex_term_bitmap", "{}", e);
        TantivySearchError::IndexSearcherError(e)
    })?;

    // Convert results to u8 bitmap.
    let row_ids_number: Vec<u32> = result.iter().collect();
    let u8_bitmap: Vec<u8> = ConvertUtils::row_ids_to_u8_bitmap(&row_ids_number);

    Ok(u8_bitmap)
}










// pub fn search_in_rowid_range(
//     index_path: &str,
//     column_name: &str,
//     query: &str,
//     lrange: u64,
//     rrange: u64,
//     use_regex: bool,
// ) -> Result<bool, TantivySearchError> {
//     if lrange > rrange {
//         return Err(TantivySearchError::InvalidArgument(format!(
//             "lrange:{}, rrange:{}",
//             lrange, rrange
//         )));
//     }

//     // Get index_reader_bridge from CACHE
//     let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()).map_err(|e|{
//         ERROR!(function:"search_in_rowid_range", "{}", e);
//         TantivySearchError::InternalError(e)
//     })?;

//     match FFiIndexSearcherUtils::perform_search_with_range(
//         &index_reader_bridge, &column_names, query, lrange, rrange, use_regex,
//     ) {
//         Ok(row_id_range) => Ok(!row_id_range.is_empty()),
//         Err(e) => {
//             ERROR!(function:"search_in_rowid_range", "{}", e);
//             return Err(TantivySearchError::IndexSearcherError(e));
//         }
//     };
//     let 
// }

// pub fn count_in_rowid_range(
//     index_path: &str,
//     column_names: &Vec<String>,
//     query: &str,
//     lrange: u64,
//     rrange: u64,
//     use_regex: bool,
// ) -> Result<u64, TantivySearchError> {
//     if lrange > rrange {
//         return Err(TantivySearchError::InvalidArgument(format!(
//             "lrange:{}, rrange:{}",
//             lrange, rrange
//         )));
//     }
//     // get index_reader_bridge from CACHE
//     let index_reader_bridge = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()).map_err(|e|{
//         ERROR!(function:"count_in_rowid_range", "{}", e);
//         TantivySearchError::InternalError(e)
//     })?;

//     match FFiIndexSearcherUtils::perform_search_with_range(
//         &index_reader_bridge, column_names, query, lrange, rrange, use_regex,
//     ) {
//         Ok(row_id_range) => Ok(row_id_range.len() as u64),
//         Err(e) => {
//             ERROR!(function:"count_in_rowid_range", "{}", e);
//             return Err(TantivySearchError::IndexSearcherError(e));
//         }
//     }
// }

// pub fn search_bitmap_results(
//     index_path: &str,
//     column_names: &Vec<String>,
//     query: &str,
//     use_regex: bool,
// ) -> Result<Vec<u8>, TantivySearchError> {
//     // get index_reader_bridge from CACHE
//     let index_reader_bridge = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()).map_err(|e|{
//         ERROR!(function:"search_bitmap_results", "{}", e);
//         TantivySearchError::InternalError(e)
//     })?;

//     let row_ids_bitmap = FFiIndexSearcherUtils::perform_search(&index_reader_bridge, &column_names, query, use_regex).map_err(|e|{
//         ERROR!(function:"search_bitmap_results", "{}", e);
//         TantivySearchError::IndexSearcherError(e)
//     })?;

//     let row_ids_number: Vec<u32> = row_ids_bitmap.iter().collect();
//     DEBUG!(function:"search_bitmap_results", "row_ids_number: {:?}", row_ids_number);
//     let u8_bitmap: Vec<u8> = ConvertUtils::row_ids_to_u8_bitmap(&row_ids_number);
//     Ok(u8_bitmap)
// }
