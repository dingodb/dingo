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

use super::strategy::query_strategy::RegexQueryStrategy;
use super::strategy::query_strategy::SentenceQueryStrategy;
use super::strategy::query_strategy::TermSetQueryStrategy;

/// Execute Term Query in specific rowid range.
pub fn query_term_with_range(
    index_path: &str,
    column_name: &str,
    term: &str,
    lrange: u64,
    rrange: u64,
) -> Result<bool, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"query_term_with_range", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    // Choose query strategy to construct query executor.
    let term_query: SingleTermQueryStrategy<'_> = SingleTermQueryStrategy { column_name, term };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&term_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor
        .execute(&index_reader_bridge.reader.searcher())
        .map_err(|e| {
            ERROR!(function:"query_term_with_range", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;

    // Intersect query results with range.
    let intersected =
        FFiIndexSearcherUtils::intersect_with_range(result, lrange, rrange).map_err(|e| {
            ERROR!(function:"query_term_with_range", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;
    Ok(!intersected.is_empty())
}

/// Execute Terms Query in specific rowid range.
pub fn query_terms_with_range(
    index_path: &str,
    column_name: &str,
    terms: &Vec<String>,
    lrange: u64,
    rrange: u64,
) -> Result<bool, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"query_term_with_range", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    // Choose query strategy to construct query executor.
    let terms_query: TermSetQueryStrategy<'_> = TermSetQueryStrategy { column_name, terms };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&terms_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor
        .execute(&index_reader_bridge.reader.searcher())
        .map_err(|e| {
            ERROR!(function:"query_terms_with_range", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;

    // Intersect query results with range.
    let intersected =
        FFiIndexSearcherUtils::intersect_with_range(result, lrange, rrange).map_err(|e| {
            ERROR!(function:"query_terms_with_range", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;
    Ok(!intersected.is_empty())
}

/// Execute Sentence Query in specific rowid range.
pub fn query_sentence_with_range(
    index_path: &str,
    column_name: &str,
    sentence: &str,
    lrange: u64,
    rrange: u64,
) -> Result<bool, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"query_term_with_range", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    // Choose query strategy to construct query executor.
    let sentence_query: SentenceQueryStrategy<'_> = SentenceQueryStrategy {
        column_name,
        sentence,
    };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&sentence_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor
        .execute(&index_reader_bridge.reader.searcher())
        .map_err(|e| {
            ERROR!(function:"query_sentence_with_range", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;

    // Intersect query results with range.
    let intersected =
        FFiIndexSearcherUtils::intersect_with_range(result, lrange, rrange).map_err(|e| {
            ERROR!(function:"query_sentence_with_range", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;
    Ok(!intersected.is_empty())
}

/// Execute Regex Query in specific rowid range.
pub fn regex_term_with_range(
    index_path: &str,
    column_name: &str,
    pattern: &str,
    lrange: u64,
    rrange: u64,
) -> Result<bool, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
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
    let result: Arc<RoaringBitmap> = query_executor
        .execute(&index_reader_bridge.reader.searcher())
        .map_err(|e| {
            ERROR!(function:"regex_term_with_range", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;

    // Intersect query results with range.
    let intersected =
        FFiIndexSearcherUtils::intersect_with_range(result, lrange, rrange).map_err(|e| {
            ERROR!(function:"regex_term_with_range", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;
    Ok(!intersected.is_empty())
}

/// Execute Term Query.
pub fn query_term_bitmap(
    index_path: &str,
    column_name: &str,
    term: &str,
) -> Result<Vec<u8>, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"query_term_bitmap", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    // Choose query strategy to construct query executor.
    let term_query: SingleTermQueryStrategy<'_> = SingleTermQueryStrategy { column_name, term };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&term_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor
        .execute(&index_reader_bridge.reader.searcher())
        .map_err(|e| {
            ERROR!(function:"query_term_bitmap", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;

    // Convert results to u8 bitmap.
    let row_ids_number: Vec<u32> = result.iter().collect();
    let u8_bitmap: Vec<u8> = ConvertUtils::row_ids_to_u8_bitmap(&row_ids_number);

    Ok(u8_bitmap)
}

/// Execute Terms Query.
pub fn query_terms_bitmap(
    index_path: &str,
    column_name: &str,
    terms: &Vec<String>,
) -> Result<Vec<u8>, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"query_terms_bitmap", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    // Choose query strategy to construct query executor.
    let sentence_query: TermSetQueryStrategy<'_> = TermSetQueryStrategy { column_name, terms };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&sentence_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor
        .execute(&index_reader_bridge.reader.searcher())
        .map_err(|e| {
            ERROR!(function:"query_terms_bitmap", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;

    // Convert results to u8 bitmap.
    let row_ids_number: Vec<u32> = result.iter().collect();
    let u8_bitmap: Vec<u8> = ConvertUtils::row_ids_to_u8_bitmap(&row_ids_number);

    Ok(u8_bitmap)
}

/// Execute Sentence Query.
pub fn query_sentence_bitmap(
    index_path: &str,
    column_name: &str,
    sentence: &str,
) -> Result<Vec<u8>, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"query_sentence_bitmap", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    // Choose query strategy to construct query executor.
    let sentence_query: SentenceQueryStrategy<'_> = SentenceQueryStrategy {
        column_name,
        sentence,
    };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&sentence_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor
        .execute(&index_reader_bridge.reader.searcher())
        .map_err(|e| {
            ERROR!(function:"query_sentence_bitmap", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;

    // Convert results to u8 bitmap.
    let row_ids_number: Vec<u32> = result.iter().collect();
    let u8_bitmap: Vec<u8> = ConvertUtils::row_ids_to_u8_bitmap(&row_ids_number);

    Ok(u8_bitmap)
}

/// Execute Regex Query.
pub fn regex_term_bitmap(
    index_path: &str,
    column_name: &str,
    pattern: &str,
) -> Result<Vec<u8>, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
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
    let result: Arc<RoaringBitmap> = query_executor
        .execute(&index_reader_bridge.reader.searcher())
        .map_err(|e| {
            ERROR!(function:"regex_term_bitmap", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;

    // Convert results to u8 bitmap.
    let row_ids_number: Vec<u32> = result.iter().collect();
    let u8_bitmap: Vec<u8> = ConvertUtils::row_ids_to_u8_bitmap(&row_ids_number);

    Ok(u8_bitmap)
}
