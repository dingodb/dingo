use crate::logger::logger_bridge::TantivySearchLogger;
use crate::tokenizer::tokenizer_utils::ToeknizerUtils;
use crate::CXX_STRING_CONERTER;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use cxx::CxxString;

pub fn ffi_varify_index_parameter(
    index_json_parameter: &CxxString,
    // TODO Add other parameters.
) -> bool {
    match CXX_STRING_CONERTER.convert(index_json_parameter) {
        Ok(json_parameter) => {
            match ToeknizerUtils::varify_json_parameter(json_parameter.as_str()) {
                Ok(valid) => valid,
                Err(e) => {
                    ERROR!(function: "ffi_varify_index_parameter", "{}", e);
                    false
                }
            }
        }
        Err(e) => {
            ERROR!(function: "ffi_varify_index_parameter", "{}", e);
            false
        }
    }
}
