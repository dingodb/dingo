use crate::cxx_vector_converter;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::implements::api_dingo_impl::bm25_search_with_column_names;
use crate::RowIdWithScore;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::{CXX_STRING_CONERTER, CXX_VECTOR_STRING_CONERTER};
use cxx::CxxString;
use cxx::CxxVector;

pub fn ffi_bm25_search_with_column_names(
    index_path: &CxxString,
    sentence: &CxxString,
    topk: u32,
    u8_aived_bitmap: &CxxVector<u8>,
    query_with_filter: bool,
    column_names: &CxxVector<CxxString>,
) -> Vec<RowIdWithScore> {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_bm25_search", "Can't convert 'index_path', message: {}", e);
            return Vec::new();
        }
    };

    let sentence: String = match CXX_STRING_CONERTER.convert(sentence) {
        Ok(q) => q,
        Err(e) => {
            ERROR!(function: "ffi_bm25_search", "Can't convert 'sentence', message: {}", e);
            return Vec::new();
        }
    };

    let u8_aived_bitmap: Vec<u8> = match cxx_vector_converter::<u8>().convert(u8_aived_bitmap) {
        Ok(bitmap) => bitmap,
        Err(e) => {
            ERROR!(function: "ffi_bm25_search", "Can't convert vector 'u8_aived_bitmap', message: {}", e);
            return Vec::new();
        }
    };

    let column_names: Vec<String> = match CXX_VECTOR_STRING_CONERTER.convert(column_names) {
        Ok(names) => names,
        Err(e) => {
            ERROR!(function: "ffi_bm25_search", "Can't convert vector 'column_names', message: {}", e);
            return Vec::new();
        }
    };

    match bm25_search_with_column_names(
        &index_path,
        &sentence,
        topk,
        &u8_aived_bitmap,
        query_with_filter,
        false,
        &column_names,
    ) {
        Ok(results) => results,
        Err(e) => {
            ERROR!(function: "ffi_bm25_search", "Error performing BM25 search with statistics: {}", e);
            Vec::new()
        }
    }
}
