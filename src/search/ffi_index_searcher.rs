use super::index_searcher_core::*;
use crate::common::errors::TantivySearchError;
use crate::cxx_vector_converter;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::RowIdWithScore;
use crate::CXX_STRING_CONERTER;
use crate::CXX_VECTOR_STRING_CONERTER;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use cxx::CxxString;
use cxx::CxxVector;

pub fn ffi_load_index(index_path: &CxxString) -> Result<bool, TantivySearchError> {
    // Parse parameter.
    let index_path: String = CXX_STRING_CONERTER.convert(index_path).map_err(|e| {
        ERROR!(function: "ffi_load_index", "Can't convert 'index_path', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    load_index(&index_path)
}

pub fn ffi_free_reader(index_path: &CxxString) -> Result<bool, TantivySearchError> {
    // Parse parameter.
    let index_path: String = CXX_STRING_CONERTER.convert(index_path).map_err(|e| {
        ERROR!(function: "ffi_free_reader", "Can't convert 'index_path', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    free_reader(&index_path)
}

pub fn ffi_search_in_rowid_range(
    index_path: &CxxString,
    column_name: &CxxString,
    query: &CxxString,
    lrange: u64,
    rrange: u64,
    use_regex: bool,
) -> Result<bool, TantivySearchError> {
    // Parse parameter.
    let index_path: String = CXX_STRING_CONERTER.convert(index_path).map_err(|e| {
        ERROR!(function: "ffi_search_in_rowid_range", "Can't convert 'index_path', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;
    let column_name: String = CXX_STRING_CONERTER.convert(query).map_err(|e| {
        ERROR!(function: "ffi_count_in_rowid_range", "Can't convert 'column_name', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;
    let query: String = CXX_STRING_CONERTER.convert(query).map_err(|e| {
        ERROR!(function: "ffi_search_in_rowid_range", "Can't convert 'query', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    let column_names: Vec<String> = vec![column_name];

    search_in_rowid_range(&index_path, &column_names, &query, lrange, rrange, use_regex)
}

pub fn ffi_count_in_rowid_range(
    index_path: &CxxString,
    column_name: &CxxString,
    query: &CxxString,
    lrange: u64,
    rrange: u64,
    use_regex: bool,
) -> Result<u64, TantivySearchError> {
    // Parse parameter.
    let index_path: String = CXX_STRING_CONERTER.convert(index_path).map_err(|e| {
        ERROR!(function: "ffi_count_in_rowid_range", "Can't convert 'index_path', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;
    let column_name: String = CXX_STRING_CONERTER.convert(query).map_err(|e| {
        ERROR!(function: "ffi_count_in_rowid_range", "Can't convert 'column_name', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;
    let query: String = CXX_STRING_CONERTER.convert(query).map_err(|e| {
        ERROR!(function: "ffi_count_in_rowid_range", "Can't convert 'query', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    let column_names: Vec<String> = vec![column_name];

    count_in_rowid_range(&index_path, &column_names ,&query, lrange, rrange, use_regex)
}

pub fn ffi_bm25_search_with_filter(
    index_path: &CxxString,
    column_names: &CxxVector<CxxString>,
    query: &CxxString,
    u8_bitmap: &CxxVector<u8>,
    top_k: u32,
    need_text: bool,
) -> Result<Vec<RowIdWithScore>, TantivySearchError> {
    // Parse parameter.
    let index_path: String = CXX_STRING_CONERTER.convert(index_path).map_err(|e|{
        ERROR!(function: "ffi_bm25_search_with_filter", "Can't convert 'index_path', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    let column_names: Vec<String> = CXX_VECTOR_STRING_CONERTER.convert(column_names).map_err(|e|{
        ERROR!(function: "ffi_bm25_search_with_filter", "Can't convert 'column_names', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    let query: String = CXX_STRING_CONERTER.convert(query).map_err(|e| {
        ERROR!(function: "ffi_bm25_search_with_filter", "Can't convert 'query', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    let u8_bitmap: Vec<u8> = cxx_vector_converter::<u8>().convert(u8_bitmap).map_err(|e|{
        ERROR!(function: "ffi_bm25_search_with_filter", "Can't convert vector 'u8_bitmap', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    bm25_search_with_filter(&index_path,&column_names, &query, &u8_bitmap, top_k, need_text)
}

pub fn ffi_bm25_search(
    index_path: &CxxString,
    column_names: &CxxVector<CxxString>,
    query: &CxxString,
    top_k: u32,
    need_text: bool,
) -> Result<Vec<RowIdWithScore>, TantivySearchError> {
    // Parse parameter.
    let index_path: String = CXX_STRING_CONERTER.convert(index_path).map_err(|e|{
        ERROR!(function: "ffi_bm25_search", "Can't convert 'index_path', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;
    let column_names: Vec<String> = CXX_VECTOR_STRING_CONERTER.convert(column_names).map_err(|e|{
        ERROR!(function: "ffi_bm25_search", "Can't convert 'column_names', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;
    let query: String = CXX_STRING_CONERTER.convert(query).map_err(|e| {
        ERROR!(function: "ffi_bm25_search", "Can't convert 'query', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    bm25_search(&index_path, &column_names, &query, top_k, need_text)
}

pub fn ffi_search_bitmap_results(
    index_path: &CxxString,
    column_names: &CxxVector<CxxString>,
    query: &CxxString,
    use_regex: bool,
) -> Result<Vec<u8>, TantivySearchError> {
    // Parse parameter.
    let index_path: String = CXX_STRING_CONERTER.convert(index_path).map_err(|e| {
        ERROR!(function: "ffi_search_bitmap_results", "Can't convert 'index_path', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;
    let column_names: Vec<String> = CXX_VECTOR_STRING_CONERTER.convert(column_names).map_err(|e|{
        ERROR!(function: "ffi_search_bitmap_results", "Can't convert 'column_names', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;
    let query: String = CXX_STRING_CONERTER.convert(query).map_err(|e| {
        ERROR!(function: "ffi_search_bitmap_results", "Can't convert 'query', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    
    search_bitmap_results(&index_path, &column_names, &query, use_regex)
}

pub fn ffi_indexed_doc_counts(index_path: &CxxString) -> Result<u64, TantivySearchError> {
    // Parse parameter.
    let index_path: String = CXX_STRING_CONERTER.convert(index_path).map_err(|e| {
        ERROR!(function: "ffi_indexed_doc_counts", "Can't convert 'index_path', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;
    indexed_doc_counts(&index_path)
}
