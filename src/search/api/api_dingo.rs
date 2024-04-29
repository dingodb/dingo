use crate::cxx_vector_converter;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::implements::api_dingo_impl::{
    bm25_search, bm25_search_with_column_names, get_doc_freq, get_total_num_docs,
    get_total_num_tokens, index_reader_reload,
};
use crate::BM25Result;
use crate::BoolResult;
use crate::DocWithFreq;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::{CXX_STRING_CONERTER, CXX_VECTOR_STRING_CONERTER};
use cxx::CxxString;
use cxx::CxxVector;

pub fn ffi_bm25_search(
    index_path: &CxxString,
    sentence: &CxxString,
    topk: u32,
    u8_aived_bitmap: &CxxVector<u8>,
    query_with_filter: bool,
) -> BM25Result {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_bm25_search", "Can't convert 'index_path', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'index_path', message: {}", e);
            return BM25Result {
                result: Vec::new(),
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    let sentence: String = match CXX_STRING_CONERTER.convert(sentence) {
        Ok(q) => q,
        Err(e) => {
            ERROR!(function: "ffi_bm25_search", "Can't convert 'sentence', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'sentence', message: {}", e);
            return BM25Result {
                result: Vec::new(),
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    let u8_aived_bitmap: Vec<u8> = match cxx_vector_converter::<u8>().convert(u8_aived_bitmap) {
        Ok(bitmap) => bitmap,
        Err(e) => {
            ERROR!(function: "ffi_bm25_search", "Can't convert vector 'u8_aived_bitmap', message: {}", e);
            let error_msg_for_cxx: String =
                format!("Can't convert vector 'u8_aived_bitmap', message: {}", e);
            return BM25Result {
                result: Vec::new(),
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    match bm25_search(
        &index_path,
        &sentence,
        topk,
        &u8_aived_bitmap,
        query_with_filter,
        false,
    ) {
        Ok(results) => {
            return BM25Result {
                result: results,
                error_code: 0,
                error_msg: String::new(),
            };
        }
        Err(e) => {
            ERROR!(function: "ffi_bm25_search", "Error performing BM25 search with statistics: {}", e);
            let error_msg_for_cxx: String =
                format!("Error performing BM25 search with statistics: {}", e);
            return BM25Result {
                result: Vec::new(),
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    }
}

pub fn ffi_bm25_search_with_column_names(
    index_path: &CxxString,
    sentence: &CxxString,
    topk: u32,
    u8_aived_bitmap: &CxxVector<u8>,
    query_with_filter: bool,
    column_names: &CxxVector<CxxString>,
) -> BM25Result {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_bm25_search", "Can't convert 'index_path', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'index_path', message: {}", e);
            return BM25Result {
                result: Vec::new(),
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    let sentence: String = match CXX_STRING_CONERTER.convert(sentence) {
        Ok(q) => q,
        Err(e) => {
            ERROR!(function: "ffi_bm25_search", "Can't convert 'sentence', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'sentence', message: {}", e);
            return BM25Result {
                result: Vec::new(),
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    let u8_aived_bitmap: Vec<u8> = match cxx_vector_converter::<u8>().convert(u8_aived_bitmap) {
        Ok(bitmap) => bitmap,
        Err(e) => {
            ERROR!(function: "ffi_bm25_search", "Can't convert vector 'u8_aived_bitmap', message: {}", e);
            let error_msg_for_cxx: String =
                format!("Can't convert vector 'u8_aived_bitmap', message: {}", e);
            return BM25Result {
                result: Vec::new(),
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    let column_names: Vec<String> = match CXX_VECTOR_STRING_CONERTER.convert(column_names) {
        Ok(names) => names,
        Err(e) => {
            ERROR!(function: "ffi_bm25_search", "Can't convert vector 'column_names', message: {}", e);
            let error_msg_for_cxx: String =
                format!("Can't convert vector 'column_names', message: {}", e);
            return BM25Result {
                result: Vec::new(),
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
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
        Ok(results) => {
            return BM25Result {
                result: results,
                error_code: 0,
                error_msg: String::new(),
            };
        }
        Err(e) => {
            ERROR!(function: "ffi_bm25_search", "Error performing BM25 search with statistics: {}", e);
            let error_msg_for_cxx: String =
                format!("Error performing BM25 search with statistics: {}", e);
            return BM25Result {
                result: Vec::new(),
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    }
}

pub fn ffi_index_reader_reload(index_path: &CxxString) -> BoolResult {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_index_reader_reload", "Can't convert 'index_path', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'index_path', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    match index_reader_reload(&index_path) {
        Ok(_) => {
            return BoolResult {
                result: true,
                error_code: 0,
                error_msg: String::new(),
            };
        }
        Err(e) => {
            ERROR!(function: "ffi_index_reader_reload", "Error reloading index reader: {}", e);
            let error_msg_for_cxx: String = format!("Error reloading index reader: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
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
