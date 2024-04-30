use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::implements::api_common_impl::free_index_reader;
use crate::search::implements::api_common_impl::get_index_json_parameter;
use crate::search::implements::api_common_impl::get_index_meta_json;
use crate::search::implements::api_common_impl::get_indexed_doc_counts;
use crate::search::implements::api_common_impl::load_index_reader;
use crate::CXX_STRING_CONERTER;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::{BoolResult, StringResult};
use cxx::CxxString;

pub fn ffi_get_index_json_parameter(index_path: &CxxString) -> StringResult {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_get_index_json_parameter", "Can't convert 'index_path', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'index_path', message: {}", e);
            return StringResult {
                result: String::new(),
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    match get_index_json_parameter(&index_path) {
        Ok(status) => StringResult {
            result: status,
            error_code: 0,
            error_msg: String::new(),
        },
        Err(e) => {
            ERROR!(function: "ffi_get_index_json_parameter", "Error get_index_json_parameter: {}", e);
            let error_msg_for_cxx: String = format!("Error get_index_json_parameter: {}", e);
            return StringResult {
                result: String::new(),
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    }
}

pub fn ffi_get_index_meta_json(index_path: &CxxString) -> StringResult {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_get_index_meta_json", "Can't convert 'index_path', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'index_path', message: {}", e);
            return StringResult {
                result: String::new(),
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    match get_index_meta_json(&index_path) {
        Ok(status) => StringResult {
            result: status,
            error_code: 0,
            error_msg: String::new(),
        },
        Err(e) => {
            ERROR!(function: "ffi_get_index_meta_json", "Error get_index_meta_json: {}", e);
            let error_msg_for_cxx: String = format!("Error get_index_meta_json: {}", e);
            return StringResult {
                result: String::new(),
                error_code: 1,
                error_msg: error_msg_for_cxx,
            };
        }
    }
}

pub fn ffi_load_index_reader(index_path: &CxxString) -> BoolResult {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_load_index_reader", "Can't convert 'index_path', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'index_path', message: {}", e);
            return BoolResult {
                result: false,
                error_code: 1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    match load_index_reader(&index_path) {
        Ok(status) => BoolResult {
            result: status,
            error_code: 0,
            error_msg: "".to_string(),
        },
        Err(e) => {
            ERROR!(function: "ffi_load_index_reader", "Error loading index reader: {}", e);
            let error_msg_for_cxx: String = format!("Error loading index reader: {}", e);
            return BoolResult {
                result: false,
                error_code: 1,
                error_msg: error_msg_for_cxx,
            };
        }
    }
}

pub fn ffi_free_index_reader(index_path: &CxxString) -> BoolResult {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_free_index_reader", "Can't convert 'index_path', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'index_path', message: {}", e);
            return BoolResult {
                result: false,
                error_code: 1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    match free_index_reader(&index_path) {
        Ok(status) => BoolResult {
            result: status,
            error_code: 0,
            error_msg: String::new(),
        },
        Err(e) => {
            ERROR!(function: "ffi_free_index_reader", "Error free index reader: {}", e);
            let error_msg_for_cxx: String = format!("Error free index reader: {}", e);
            return BoolResult {
                result: false,
                error_code: 1,
                error_msg: error_msg_for_cxx,
            };
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
