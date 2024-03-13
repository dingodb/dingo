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
) -> Result<bool, String> {

    let index_json_parameter: String = CXX_STRING_CONERTER.convert(index_json_parameter).map_err(|e|{
        ERROR!(function: "ffi_varify_index_parameter", "Can't convert 'index_json_parameter', message: {}", e);
        TantivySearchError::CxxConvertError(e).to_string()
    })?;

    ToeknizerUtils::varify_json_parameter(index_json_parameter.as_str()).map_err(|e|{
        e.to_string()
    })
}