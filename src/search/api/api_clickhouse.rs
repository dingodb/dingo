use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::implements::api_clickhouse_impl::query_sentence_bitmap;
use crate::search::implements::api_clickhouse_impl::query_sentence_with_range;
use crate::search::implements::api_clickhouse_impl::query_term_bitmap;
use crate::search::implements::api_clickhouse_impl::query_term_with_range;
use crate::search::implements::api_clickhouse_impl::query_terms_bitmap;
use crate::search::implements::api_clickhouse_impl::query_terms_with_range;
use crate::search::implements::api_clickhouse_impl::regex_term_bitmap;
use crate::search::implements::api_clickhouse_impl::regex_term_with_range;
use crate::CXX_STRING_CONERTER;
use crate::CXX_VECTOR_STRING_CONERTER;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use cxx::CxxString;
use cxx::CxxVector;

pub fn ffi_query_term_with_range(
    index_path: &CxxString,
    column_name: &CxxString,
    term: &CxxString,
    lrange: u64,
    rrange: u64,
) -> bool {
    if lrange > rrange {
        ERROR!(function: "ffi_query_term_with_range", "range is invalid: [{} - {}]", lrange, rrange);
        return false;
    }
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_query_term_with_range", "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };
    let column_name: String = match CXX_STRING_CONERTER.convert(column_name) {
        Ok(name) => name,
        Err(e) => {
            ERROR!(function: "ffi_query_term_with_range", "Can't convert 'column_name', message: {}", e);
            return false;
        }
    };
    let term: String = match CXX_STRING_CONERTER.convert(term) {
        Ok(q) => q,
        Err(e) => {
            ERROR!(function: "ffi_query_term_with_range", "Can't convert 'term', message: {}", e);
            return false;
        }
    };

    match query_term_with_range(&index_path, &column_name, &term, lrange, rrange) {
        Ok(exist) => exist,
        Err(e) => {
            ERROR!(function: "ffi_query_term_with_range", "Error happend. {}", e);
            false
        }
    }
}

pub fn ffi_query_terms_with_range(
    index_path: &CxxString,
    column_name: &CxxString,
    terms: &CxxVector<CxxString>,
    lrange: u64,
    rrange: u64,
) -> bool {
    if lrange > rrange {
        ERROR!(function: "ffi_query_terms_with_range", "range is invalid: [{} - {}]", lrange, rrange);
        return false;
    }
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_query_terms_with_range", "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };
    let column_name: String = match CXX_STRING_CONERTER.convert(column_name) {
        Ok(name) => name,
        Err(e) => {
            ERROR!(function: "ffi_query_terms_with_range", "Can't convert 'column_name', message: {}", e);
            return false;
        }
    };
    let terms = match CXX_VECTOR_STRING_CONERTER.convert(terms) {
        Ok(t) => t,
        Err(e) => {
            ERROR!(function: "ffi_query_terms_with_range", "Can't convert 'terms', message: {}", e);
            return false;
        }
    };

    match query_terms_with_range(&index_path, &column_name, &terms, lrange, rrange) {
        Ok(exist) => exist,
        Err(e) => {
            ERROR!(function: "ffi_query_terms_with_range", "Error happend. {}", e);
            false
        }
    }
}

pub fn ffi_query_sentence_with_range(
    index_path: &CxxString,
    column_name: &CxxString,
    sentence: &CxxString,
    lrange: u64,
    rrange: u64,
) -> bool {
    if lrange > rrange {
        ERROR!(function: "ffi_query_sentence_with_range", "range is invalid: [{} - {}]", lrange, rrange);
        return false;
    }
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_query_sentence_with_range", "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };
    let column_name = match CXX_STRING_CONERTER.convert(column_name) {
        Ok(names) => names,
        Err(e) => {
            ERROR!(function: "ffi_query_sentence_with_range", "Can't convert 'column_name', message: {}", e);
            return false;
        }
    };
    let sentence = match CXX_STRING_CONERTER.convert(sentence) {
        Ok(q) => q,
        Err(e) => {
            ERROR!(function: "ffi_query_sentence_with_range", "Can't convert 'sentence', message: {}", e);
            return false;
        }
    };

    match query_sentence_with_range(&index_path, &column_name, &sentence, lrange, rrange) {
        Ok(exist) => exist,
        Err(e) => {
            ERROR!(function: "ffi_query_sentence_with_range", "Error happend. {}", e);
            false
        }
    }
}

pub fn ffi_regex_term_with_range(
    index_path: &CxxString,
    column_name: &CxxString,
    pattern: &CxxString,
    lrange: u64,
    rrange: u64,
) -> bool {
    if lrange > rrange {
        ERROR!(function: "ffi_regex_term_with_range", "range is invalid: [{} - {}]", lrange, rrange);
        return false;
    }
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_regex_term_with_range", "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };
    let column_name: String = match CXX_STRING_CONERTER.convert(column_name) {
        Ok(name) => name,
        Err(e) => {
            ERROR!(function: "ffi_regex_term_with_range", "Can't convert 'column_name', message: {}", e);
            return false;
        }
    };
    let pattern: String = match CXX_STRING_CONERTER.convert(pattern) {
        Ok(q) => q,
        Err(e) => {
            ERROR!(function: "ffi_regex_term_with_range", "Can't convert 'pattern', message: {}", e);
            return false;
        }
    };

    match regex_term_with_range(&index_path, &column_name, &pattern, lrange, rrange) {
        Ok(exist) => exist,
        Err(e) => {
            ERROR!(function: "ffi_regex_term_with_range", "Error happend. {}", e);
            false
        }
    }
}

pub fn ffi_query_term_bitmap(
    index_path: &CxxString,
    column_name: &CxxString,
    term: &CxxString,
) -> Vec<u8> {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_query_term_bitmap", "Can't convert 'index_path', message: {}", e);
            return Vec::new();
        }
    };
    let column_name: String = match CXX_STRING_CONERTER.convert(column_name) {
        Ok(name) => name,
        Err(e) => {
            ERROR!(function: "ffi_query_term_bitmap", "Can't convert 'column_name', message: {}", e);
            return Vec::new();
        }
    };
    let term: String = match CXX_STRING_CONERTER.convert(term) {
        Ok(q) => q,
        Err(e) => {
            ERROR!(function: "ffi_query_term_bitmap", "Can't convert 'term', message: {}", e);
            return Vec::new();
        }
    };

    match query_term_bitmap(&index_path, &column_name, &term) {
        Ok(status) => status,
        Err(e) => {
            ERROR!(function: "ffi_query_term_bitmap", "Error happend. {}", e);
            Vec::new()
        }
    }
}

pub fn ffi_query_terms_bitmap(
    index_path: &CxxString,
    column_name: &CxxString,
    terms: &CxxVector<CxxString>,
) -> Vec<u8> {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_query_terms_bitmap", "Can't convert 'index_path', message: {}", e);
            return Vec::new();
        }
    };
    let column_name: String = match CXX_STRING_CONERTER.convert(column_name) {
        Ok(name) => name,
        Err(e) => {
            ERROR!(function: "ffi_query_terms_bitmap", "Can't convert 'column_name', message: {}", e);
            return Vec::new();
        }
    };
    let terms: Vec<String> = match CXX_VECTOR_STRING_CONERTER.convert(terms) {
        Ok(ts) => ts,
        Err(e) => {
            ERROR!(function: "ffi_query_terms_bitmap", "Can't convert 'terms', message: {}", e);
            return Vec::new();
        }
    };

    match query_terms_bitmap(&index_path, &column_name, &terms) {
        Ok(status) => status,
        Err(e) => {
            ERROR!(function: "ffi_query_terms_bitmap", "Error happend. {}", e);
            Vec::new()
        }
    }
}

pub fn ffi_query_sentence_bitmap(
    index_path: &CxxString,
    column_name: &CxxString,
    sentence: &CxxString,
) -> Vec<u8> {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_query_sentence_bitmap", "Can't convert 'index_path', message: {}", e);
            return Vec::new();
        }
    };
    let column_name: String = match CXX_STRING_CONERTER.convert(column_name) {
        Ok(name) => name,
        Err(e) => {
            ERROR!(function: "ffi_query_sentence_bitmap", "Can't convert 'column_name', message: {}", e);
            return Vec::new();
        }
    };
    let sentence: String = match CXX_STRING_CONERTER.convert(sentence) {
        Ok(se) => se,
        Err(e) => {
            ERROR!(function: "ffi_query_sentence_bitmap", "Can't convert 'sentence', message: {}", e);
            return Vec::new();
        }
    };

    match query_sentence_bitmap(&index_path, &column_name, &sentence) {
        Ok(status) => status,
        Err(e) => {
            ERROR!(function: "ffi_query_sentence_bitmap", "Error happend. {}", e);
            Vec::new()
        }
    }
}

pub fn ffi_regex_term_bitmap(
    index_path: &CxxString,
    column_name: &CxxString,
    pattern: &CxxString,
) -> Vec<u8> {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_regex_term_bitmap", "Can't convert 'index_path', message: {}", e);
            return Vec::new();
        }
    };
    let column_name: String = match CXX_STRING_CONERTER.convert(column_name) {
        Ok(name) => name,
        Err(e) => {
            ERROR!(function: "ffi_regex_term_bitmap", "Can't convert 'column_name', message: {}", e);
            return Vec::new();
        }
    };
    let pattern: String = match CXX_STRING_CONERTER.convert(pattern) {
        Ok(se) => se,
        Err(e) => {
            ERROR!(function: "ffi_regex_term_bitmap", "Can't convert 'pattern', message: {}", e);
            return Vec::new();
        }
    };

    match regex_term_bitmap(&index_path, &column_name, &pattern) {
        Ok(status) => status,
        Err(e) => {
            ERROR!(function: "ffi_regex_term_bitmap", "Error happend. {}", e);
            Vec::new()
        }
    }
}
