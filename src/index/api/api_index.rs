use crate::index::implements::api_index_impl::*;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::BoolResult;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::{cxx_vector_converter, CXX_STRING_CONERTER, CXX_VECTOR_STRING_CONERTER};
use cxx::{CxxString, CxxVector};

pub fn ffi_create_index_with_parameter(
    index_path: &CxxString,
    column_names: &CxxVector<CxxString>,
    index_json_parameter: &CxxString,
) -> BoolResult {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_create_index_with_parameter", "Can't convert 'index_path', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'index_path', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    let column_names: Vec<String> = match CXX_VECTOR_STRING_CONERTER.convert(column_names) {
        Ok(names) => names,
        Err(e) => {
            ERROR!(function: "ffi_create_index_with_parameter", "Can't convert 'column_names', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'column_names', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    let index_json_parameter: String = match CXX_STRING_CONERTER.convert(index_json_parameter) {
        Ok(json) => json,
        Err(e) => {
            ERROR!(function: "ffi_create_index_with_parameter", "Can't convert 'index_json_parameter', message: {}", e);
            let error_msg_for_cxx: String =
                format!("Can't convert 'index_json_parameter', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    match create_index_with_parameter(&index_path, &column_names, &index_json_parameter) {
        Ok(status) => BoolResult {
            result: status,
            error_code: 0,
            error_msg: String::new(),
        },
        Err(e) => {
            ERROR!(function: "ffi_create_index_with_parameter", "Error creating index: {}", e);
            let error_msg_for_cxx: String = format!("Error creating index: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    }
}

pub fn ffi_create_index(index_path: &CxxString, column_names: &CxxVector<CxxString>) -> BoolResult {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_create_index", "Can't convert 'index_path', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'index_path', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    let column_names: Vec<String> = match CXX_VECTOR_STRING_CONERTER.convert(column_names) {
        Ok(names) => names,
        Err(e) => {
            ERROR!(function: "ffi_create_index", "Can't convert 'column_names', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'column_names', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    match create_index(&index_path, &column_names) {
        Ok(status) => BoolResult {
            result: status,
            error_code: 0,
            error_msg: String::new(),
        },
        Err(e) => {
            ERROR!(function: "ffi_create_index", "Error creating index: {}", e);
            let error_msg_for_cxx: String = format!("Error creating index: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    }
}

pub fn ffi_index_multi_column_docs(
    index_path: &CxxString,
    row_id: u64,
    column_names: &CxxVector<CxxString>,
    column_docs: &CxxVector<CxxString>,
) -> BoolResult {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_index_multi_column_docs", "Can't convert 'index_path', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'index_path', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    let column_names: Vec<String> = match CXX_VECTOR_STRING_CONERTER.convert(column_names) {
        Ok(names) => names,
        Err(e) => {
            ERROR!(function: "ffi_index_multi_column_docs", "Can't convert 'column_names', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'column_names', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    let column_docs: Vec<String> = match CXX_VECTOR_STRING_CONERTER.convert(column_docs) {
        Ok(docs) => docs,
        Err(e) => {
            ERROR!(function: "ffi_index_multi_column_docs", "Can't convert 'column_docs', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'column_docs', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    if column_names.len() != column_docs.len() {
        ERROR!(function: "ffi_index_multi_column_docs", "column_names size doesn't match column_docs size");
        let error_msg_for_cxx: String =
            "column_names size doesn't match column_docs size".to_string();
        return BoolResult {
            result: false,
            error_code: -1,
            error_msg: error_msg_for_cxx,
        };
    }

    if column_names.len() == 0 || column_docs.len() == 0 {
        ERROR!(function: "ffi_index_multi_column_docs", "column_names and column_docs can't be empty");
        let error_msg_for_cxx: String = "column_names and column_docs can't be empty".to_string();
        return BoolResult {
            result: false,
            error_code: -1,
            error_msg: error_msg_for_cxx,
        };
    }

    match index_multi_column_docs(&index_path, row_id, &column_names, &column_docs) {
        Ok(status) => BoolResult {
            result: status,
            error_code: 0,
            error_msg: String::new(),
        },
        Err(e) => {
            ERROR!(function: "ffi_index_multi_column_docs", "Error indexing multi-column docs: {}", e);
            let error_msg_for_cxx: String = format!("Error indexing multi-column docs: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    }
}

pub fn ffi_index_multi_type_column_docs(
    index_path: &CxxString,
    row_id: u64,
    text_column_names: &CxxVector<CxxString>,
    text_column_docs: &CxxVector<CxxString>,
    i64_column_names: &CxxVector<CxxString>,
    i64_column_docs: &CxxVector<i64>,
    f64_column_names: &CxxVector<CxxString>,
    f64_column_docs: &CxxVector<f64>,
) -> BoolResult {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_index_multi_column_docs", "Can't convert 'index_path', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'index_path', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    let text_column_names: Vec<String> = match CXX_VECTOR_STRING_CONERTER.convert(text_column_names)
    {
        Ok(names) => names,
        Err(e) => {
            ERROR!(function: "ffi_index_multi_column_docs", "Can't convert 'text_column_names', message: {}", e);
            let error_msg_for_cxx: String =
                format!("Can't convert 'text_column_names', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    let text_column_docs: Vec<String> = match CXX_VECTOR_STRING_CONERTER.convert(text_column_docs) {
        Ok(docs) => docs,
        Err(e) => {
            ERROR!(function: "ffi_index_multi_column_docs", "Can't convert 'text_column_docs', message: {}", e);
            let error_msg_for_cxx: String =
                format!("Can't convert 'text_column_docs', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    if text_column_names.len() != text_column_docs.len() {
        ERROR!(function: "ffi_index_multi_column_docs", "text_column_names size doesn't match text_column_docs size");
        let error_msg_for_cxx: String =
            "text_column_names size doesn't match text_column_docs size".to_string();
        return BoolResult {
            result: false,
            error_code: -1,
            error_msg: error_msg_for_cxx,
        };
    }

    let i64_column_names: Vec<String> = match CXX_VECTOR_STRING_CONERTER.convert(i64_column_names) {
        Ok(names) => names,
        Err(e) => {
            ERROR!(function: "ffi_index_multi_column_docs", "Can't convert 'i64_column_names', message: {}", e);
            let error_msg_for_cxx: String =
                format!("Can't convert 'i64_column_names', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    // convert CxxVector<i64> to Vec<i64>
    let i64_column_docs = match cxx_vector_converter::<i64>().convert(i64_column_docs) {
        Ok(docs) => docs,
        Err(e) => {
            ERROR!(function: "ffi_index_multi_column_docs", "Can't convert 'i64_column_docs', message: {}", e);
            let error_msg_for_cxx: String =
                format!("Can't convert 'i64_column_docs', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    if i64_column_names.len() != i64_column_docs.len() {
        ERROR!(function: "ffi_index_multi_column_docs", "i64_column_names size doesn't match i64_column_docs size");
        let error_msg_for_cxx: String =
            "i64_column_names size doesn't match i64_column_docs size".to_string();
        return BoolResult {
            result: false,
            error_code: -1,
            error_msg: error_msg_for_cxx,
        };
    }

    let f64_column_names: Vec<String> = match CXX_VECTOR_STRING_CONERTER.convert(f64_column_names) {
        Ok(names) => names,
        Err(e) => {
            ERROR!(function: "ffi_index_multi_column_docs", "Can't convert 'f64_column_names', message: {}", e);
            let error_msg_for_cxx: String =
                format!("Can't convert 'f64_column_names', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    // convert CxxVector<f64> to Vec<f64>
    let f64_column_docs = match cxx_vector_converter::<f64>().convert(f64_column_docs) {
        Ok(docs) => docs,
        Err(e) => {
            ERROR!(function: "ffi_index_multi_column_docs", "Can't convert 'f64_column_docs', message: {}", e);
            let error_msg_for_cxx: String =
                format!("Can't convert 'f64_column_docs', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    if f64_column_names.len() != f64_column_docs.len() {
        ERROR!(function: "ffi_index_multi_column_docs", "f64_column_names size doesn't match f64_column_docs size");
        let error_msg_for_cxx: String =
            "f64_column_names size doesn't match f64_column_docs size".to_string();
        return BoolResult {
            result: false,
            error_code: -1,
            error_msg: error_msg_for_cxx,
        };
    }

    if (text_column_names.len() + i64_column_names.len() + f64_column_names.len()) == 0
        || (text_column_docs.len() + i64_column_docs.len() + f64_column_docs.len()) == 0
    {
        ERROR!(function: "ffi_index_multi_column_docs", "column_names and column_docs can't be empty");
        let error_msg_for_cxx: String = "column_names and column_docs can't be empty".to_string();
        return BoolResult {
            result: false,
            error_code: -1,
            error_msg: error_msg_for_cxx,
        };
    }

    match index_multi_type_column_docs(
        &index_path,
        row_id,
        &text_column_names,
        &text_column_docs,
        &i64_column_names,
        &i64_column_docs,
        &f64_column_names,
        &f64_column_docs,
    ) {
        Ok(status) => BoolResult {
            result: status,
            error_code: 0,
            error_msg: String::new(),
        },
        Err(e) => {
            ERROR!(function: "ffi_index_multi_column_docs", "Error indexing multi-column docs: {}", e);
            let error_msg_for_cxx: String = format!("Error indexing multi-column docs: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    }
}

pub fn ffi_delete_row_ids(index_path: &CxxString, row_ids: &CxxVector<u32>) -> BoolResult {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_delete_row_ids", "Can't convert 'index_path', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'index_path', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    let row_ids: Vec<u32> = match cxx_vector_converter::<u32>().convert(row_ids) {
        Ok(ids) => ids,
        Err(e) => {
            ERROR!(function: "ffi_delete_row_ids", "Can't convert 'row_ids', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'row_ids', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    match delete_row_ids(&index_path, &row_ids) {
        Ok(status) => BoolResult {
            result: status,
            error_code: 0,
            error_msg: String::new(),
        },
        Err(e) => {
            ERROR!(function: "ffi_delete_row_ids", "Error deleting row ids: {}", e);
            let error_msg_for_cxx: String = format!("Error deleting row ids: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    }
}

pub fn ffi_index_writer_commit(index_path: &CxxString) -> BoolResult {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_index_writer_commit", "Can't convert 'index_path', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'index_path', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    match commit_index(&index_path) {
        Ok(status) => BoolResult {
            result: status,
            error_code: 0,
            error_msg: String::new(),
        },
        Err(e) => {
            ERROR!(function: "ffi_index_writer_commit", "Error committing index: {}", e);
            let error_msg_for_cxx: String = format!("Error committing index: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    }
}

pub fn ffi_free_index_writer(index_path: &CxxString) -> BoolResult {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_free_index_writer", "Can't convert 'index_path', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'index_path', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    match free_index_writer(&index_path) {
        Ok(status) => BoolResult {
            result: status,
            error_code: 0,
            error_msg: String::new(),
        },
        Err(e) => {
            ERROR!(function: "ffi_free_index_writer", "Error freeing index writer: {}", e);
            let error_msg_for_cxx: String = format!("Error freeing index writer: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    }
}

pub fn ffi_load_index_writer(index_path: &CxxString) -> BoolResult {
    let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: "ffi_load_index_writer", "Can't convert 'index_path', message: {}", e);
            let error_msg_for_cxx: String = format!("Can't convert 'index_path', message: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    };

    match load_index_writer(&index_path) {
        Ok(status) => {
            return BoolResult {
                result: status,
                error_code: 0,
                error_msg: String::new(),
            };
        }
        Err(e) => {
            ERROR!(function: "ffi_load_index_writer", "Error loading index reader: {}", e);
            let error_msg_for_cxx: String = format!("Error loading index reader: {}", e);
            return BoolResult {
                result: false,
                error_code: -1,
                error_msg: error_msg_for_cxx,
            };
        }
    }
}
