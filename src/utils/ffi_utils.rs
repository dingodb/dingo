use crate::common::errors::{TantivySearchError, TokenizerUtilsError};
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::tokenizer::tokenizer_utils::ToeknizerUtils;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::{CXX_STRING_CONERTER, CXX_VECTOR_STRING_CONERTER};
use cxx::{CxxString, CxxVector};

use super::index_utils::IndexUtils;

/// 验证索引创建参数是否合法
pub fn ffi_varify_index_parameter(
    index_json_parameter: &CxxString,
    // TODO 其它的参数验证
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