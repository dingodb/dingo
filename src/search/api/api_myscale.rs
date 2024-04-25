use crate::cxx_vector_converter;
use crate::ffi::Statistics;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::implements::api_myscale_impl::bm25_search;
use crate::search::implements::api_myscale_impl::get_doc_freq;
use crate::search::implements::api_myscale_impl::get_total_num_docs;
use crate::search::implements::api_myscale_impl::get_total_num_tokens;
use crate::DocWithFreq;
use crate::RowIdWithScore;
use crate::CXX_STRING_CONERTER;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use cxx::CxxString;
use cxx::CxxVector;

pub fn ffi_bm25_search(
    index_path: &CxxString,
    sentence: &CxxString,
    topk: u32,
    u8_aived_bitmap: &CxxVector<u8>,
    query_with_filter: bool,
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

    let statistics = Statistics {
        docs_freq: Vec::new(),
        total_num_tokens: 0,
        total_num_docs: 0,
    };

    match bm25_search(
        &index_path,
        &sentence,
        topk,
        &u8_aived_bitmap,
        query_with_filter,
        &statistics,
        false,
    ) {
        Ok(results) => results,
        Err(e) => {
            ERROR!(function: "ffi_bm25_search", "Error performing BM25 search with statistics: {}", e);
            Vec::new()
        }
    }
}

pub fn ffi_bm25_search_with_stat(
    index_path: &CxxString,
    sentence: &CxxString,
    topk: u32,
    u8_aived_bitmap: &CxxVector<u8>,
    query_with_filter: bool,
    statistics: &Statistics,
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

    match bm25_search(
        &index_path,
        &sentence,
        topk,
        &u8_aived_bitmap,
        query_with_filter,
        statistics,
        false,
    ) {
        Ok(results) => results,
        Err(e) => {
            ERROR!(function: "ffi_bm25_search", "Error performing BM25 search with statistics: {}", e);
            Vec::new()
        }
    }
}

pub fn ffi_get_doc_freq(index_path: &CxxString, sentence: &CxxString) -> Vec<DocWithFreq> {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_get_doc_freq", "Can't convert 'index_path', message: {}", e);
            return Vec::new();
        }
    };

    let sentence: String = match CXX_STRING_CONERTER.convert(sentence) {
        Ok(q) => q,
        Err(e) => {
            ERROR!(function: "ffi_get_doc_freq", "Can't convert 'sentence', message: {}", e);
            return Vec::new();
        }
    };

    match get_doc_freq(&index_path, &sentence) {
        Ok(results) => results,
        Err(e) => {
            ERROR!(function: "ffi_get_doc_freq", "Error performing get_doc_freq: {}", e);
            Vec::new()
        }
    }
}

pub fn ffi_get_total_num_docs(index_path: &CxxString) -> u64 {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_get_total_num_docs", "Can't convert 'index_path', message: {}", e);
            return 0u64;
        }
    };
    match get_total_num_docs(&index_path) {
        Ok(results) => results,
        Err(e) => {
            ERROR!(function: "ffi_get_total_num_docs", "Error performing get_total_num_docs: {}", e);
            0u64
        }
    }
}

pub fn ffi_get_total_num_tokens(index_path: &CxxString) -> u64 {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_get_total_num_tokens", "Can't convert 'index_path', message: {}", e);
            return 0u64;
        }
    };

    match get_total_num_tokens(&index_path) {
        Ok(results) => results,
        Err(e) => {
            ERROR!(function: "ffi_get_total_num_tokens", "Error performing get_total_num_tokens: {}", e);
            0u64
        }
    }
}
