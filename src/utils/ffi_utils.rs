use crate::logger::logger_bridge::TantivySearchLogger;
use crate::tokenizer::tokenizer_utils::TokenizerUtils;
use crate::BoolResult;
use crate::CXX_STRING_CONERTER;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use cxx::CxxString;

pub fn ffi_varify_index_parameter(index_json_parameter: &CxxString) -> BoolResult {
    match CXX_STRING_CONERTER.convert(index_json_parameter) {
        Ok(json_parameter) => {
            match TokenizerUtils::varify_json_parameter(json_parameter.as_str()) {
                Ok(valid) => BoolResult {
                    result: valid,
                    error_code: 0,
                    error_msg: String::new(),
                },
                Err(e) => {
                    ERROR!(function: "ffi_varify_index_parameter", "{}", e);
                    let error: String = format!("Error varify index parameter: {}", e);
                    BoolResult {
                        result: false,
                        error_code: 1,
                        error_msg: error,
                    }
                }
            }
        }
        Err(e) => {
            ERROR!(function: "ffi_varify_index_parameter", "{}", e);
            let error: String = format!("Error convert index parameter: {}", e);
            BoolResult {
                result: false,
                error_code: 1,
                error_msg: error,
            }
        }
    }
}
