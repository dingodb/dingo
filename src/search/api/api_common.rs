use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::implements::api_common_impl::free_index_reader;
use crate::search::implements::api_common_impl::get_indexed_doc_counts;
use crate::search::implements::api_common_impl::load_index_reader;
use crate::CXX_STRING_CONERTER;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use cxx::CxxString;



pub fn ffi_load_index_reader(index_path: &CxxString) -> bool {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_load_index_reader", "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };

    match load_index_reader(&index_path) {
        Ok(status) => status,
        Err(e) => {
            ERROR!(function: "ffi_load_index_reader", "Error loading index reader: {}", e);
            false
        }
    }
}


pub fn ffi_free_index_reader(index_path: &CxxString) -> bool {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_free_index_reader", "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };

    match free_index_reader(&index_path) {
        Ok(status) => status,
        Err(e) => {
            ERROR!(function: "ffi_free_index_reader", "Error free index reader: {}", e);
            false
        }
    }
}


pub fn ffi_get_indexed_doc_counts(index_path: &CxxString) -> u64 {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_get_indexed_doc_counts", "Can't convert 'index_path', message: {}", e);
            return 0;
        }
    };

    match get_indexed_doc_counts(&index_path) {
        Ok(count) => count,
        Err(e) => {
            ERROR!(function: "ffi_get_indexed_doc_counts", "Error getting indexed doc counts: {}", e);
            0
        }
    }
}