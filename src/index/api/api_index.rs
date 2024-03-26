use crate::index::implements::api_index_impl::*;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::{cxx_vector_converter, CXX_STRING_CONERTER, CXX_VECTOR_STRING_CONERTER};
use cxx::{CxxString, CxxVector};

pub fn ffi_create_index_with_parameter(
    index_path: &CxxString,
    column_names: &CxxVector<CxxString>,
    index_json_parameter: &CxxString,
) -> bool {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_create_index_with_parameter", "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };

    let column_names: Vec<String> = match CXX_VECTOR_STRING_CONERTER.convert(column_names) {
        Ok(names) => names,
        Err(e) => {
            ERROR!(function: "ffi_create_index_with_parameter", "Can't convert 'column_names', message: {}", e);
            return false;
        }
    };

    let index_json_parameter: String = match CXX_STRING_CONERTER.convert(index_json_parameter) {
        Ok(json) => json,
        Err(e) => {
            ERROR!(function: "ffi_create_index_with_parameter", "Can't convert 'index_json_parameter', message: {}", e);
            return false;
        }
    };

    match create_index_with_parameter(&index_path, &column_names, &index_json_parameter) {
        Ok(status) => status,
        Err(e) => {
            ERROR!(function: "ffi_create_index_with_parameter", "Error creating index: {}", e);
            false
        }
    }
}

pub fn ffi_create_index(index_path: &CxxString, column_names: &CxxVector<CxxString>) -> bool {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_create_index", "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };

    let column_names: Vec<String> = match CXX_VECTOR_STRING_CONERTER.convert(column_names) {
        Ok(names) => names,
        Err(e) => {
            ERROR!(function: "ffi_create_index", "Can't convert 'column_names', message: {}", e);
            return false;
        }
    };

    match create_index(&index_path, &column_names) {
        Ok(status) => status,
        Err(e) => {
            ERROR!(function: "ffi_create_index", "Error creating index: {}", e);
            false
        }
    }
}

pub fn ffi_index_multi_column_docs(
    index_path: &CxxString,
    row_id: u64,
    column_names: &CxxVector<CxxString>,
    column_docs: &CxxVector<CxxString>,
) -> bool {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_index_multi_column_docs", "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };

    let column_names: Vec<String> = match CXX_VECTOR_STRING_CONERTER.convert(column_names) {
        Ok(names) => names,
        Err(e) => {
            ERROR!(function: "ffi_index_multi_column_docs", "Can't convert 'column_names', message: {}", e);
            return false;
        }
    };

    let column_docs: Vec<String> = match CXX_VECTOR_STRING_CONERTER.convert(column_docs) {
        Ok(docs) => docs,
        Err(e) => {
            ERROR!(function: "ffi_index_multi_column_docs", "Can't convert 'column_docs', message: {}", e);
            return false;
        }
    };

    if column_names.len() != column_docs.len() {
        ERROR!(function: "ffi_index_multi_column_docs", "column_names size doesn't match column_docs size");
        return false;
    }

    if column_names.len() == 0 || column_docs.len() == 0 {
        ERROR!(function: "ffi_index_multi_column_docs", "column_names and column_docs can't be empty");
        return false;
    }

    match index_multi_column_docs(&index_path, row_id, &column_names, &column_docs) {
        Ok(status) => status,
        Err(e) => {
            ERROR!(function: "ffi_index_multi_column_docs", "Error indexing multi-column docs: {}", e);
            false
        }
    }
}

pub fn ffi_delete_row_ids(index_path: &CxxString, row_ids: &CxxVector<u32>) -> bool {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_delete_row_ids", "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };

    let row_ids: Vec<u32> = match cxx_vector_converter::<u32>().convert(row_ids) {
        Ok(ids) => ids,
        Err(e) => {
            ERROR!(function: "ffi_delete_row_ids", "Can't convert 'row_ids', message: {}", e);
            return false;
        }
    };

    match delete_row_ids(&index_path, &row_ids) {
        Ok(status) => status,
        Err(e) => {
            ERROR!(function: "ffi_delete_row_ids", "Error deleting row ids: {}", e);
            false
        }
    }
}

pub fn ffi_index_writer_commit(index_path: &CxxString) -> bool {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_index_writer_commit", "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };

    match commit_index(&index_path) {
        Ok(status) => status,
        Err(e) => {
            ERROR!(function: "ffi_index_writer_commit", "Error committing index: {}", e);
            false
        }
    }
}

pub fn ffi_free_index_writer(index_path: &CxxString) -> bool {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_free_index_writer", "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };

    match free_index_writer(&index_path) {
        Ok(status) => status,
        Err(e) => {
            ERROR!(function: "ffi_free_index_writer", "Error freeing index writer: {}", e);
            false
        }
    }
}
