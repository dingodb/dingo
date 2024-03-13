use crate::common::errors::TantivySearchError;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::{CXX_STRING_CONERTER, CXX_VECTOR_STRING_CONERTER};
use cxx::{CxxString, CxxVector};

use super::index_manager_core::{
    commit_index, create_index, create_index_with_parameter, delete_row_ids, free_index_writer,
    index_multi_column_docs,
};

/// 创建索引，提供索引参数
pub fn ffi_create_index_with_parameter(
    index_path: &CxxString,
    column_names: &CxxVector<CxxString>,
    index_json_parameter: &CxxString,
) -> Result<bool, TantivySearchError> {
    let index_path: String = CXX_STRING_CONERTER.convert(index_path).map_err(|e|{
        ERROR!(function: "ffi_create_index_with_parameter", "Can't convert 'index_path', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    let column_names: Vec<String> = CXX_VECTOR_STRING_CONERTER.convert(column_names).map_err(|e|{
        ERROR!(function: "ffi_create_index_with_parameter", "Can't convert 'column_names', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    let index_json_parameter: String = CXX_STRING_CONERTER.convert(index_json_parameter).map_err(|e|{
        ERROR!(function: "ffi_create_index_with_parameter", "Can't convert 'index_json_parameter', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    create_index_with_parameter(&index_path, &column_names, &index_json_parameter)
}

/// 创建索引，不提供索引参数
pub fn ffi_create_index(
    index_path: &CxxString,
    column_names: &CxxVector<CxxString>,
) -> Result<bool, TantivySearchError> {
    let index_path: String = CXX_STRING_CONERTER.convert(index_path).map_err(|e| {
        ERROR!(function: "ffi_create_index", "Can't convert 'index_path', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    let column_names: Vec<String> = CXX_VECTOR_STRING_CONERTER.convert(column_names).map_err(|e|{
        ERROR!(function: "ffi_create_index_with_parameter", "Can't convert 'column_names', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    create_index(&index_path, &column_names)
}

/// 索引一组文档
pub fn ffi_index_multi_column_docs(
    index_path: &CxxString,
    row_id: u64,
    column_names: &CxxVector<CxxString>,
    column_docs: &CxxVector<CxxString>,
) -> Result<bool, TantivySearchError> {
    let index_path: String = CXX_STRING_CONERTER.convert(index_path).map_err(|e|{
        ERROR!(function: "ffi_index_multi_column_docs", "Can't convert 'index_path', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    let column_names: Vec<String> = CXX_VECTOR_STRING_CONERTER.convert(column_names).map_err(|e|{
        ERROR!(function: "ffi_index_multi_column_docs", "Can't convert 'column_names', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    let column_docs: Vec<String> = CXX_VECTOR_STRING_CONERTER.convert(column_docs).map_err(|e|{
        ERROR!(function: "ffi_index_multi_column_docs", "Can't convert 'column_docs', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    index_multi_column_docs(&index_path, row_id, &column_names, &column_docs)
}

/// 删除一组 row ids
pub fn ffi_delete_row_ids(
    index_path: &CxxString,
    row_ids: &CxxVector<u32>,
) -> Result<bool, TantivySearchError> {
    let index_path: String = CXX_STRING_CONERTER.convert(index_path).map_err(|e| {
        ERROR!(function: "ffi_delete_row_ids", "Can't convert 'index_path', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;

    let row_ids: Vec<u32> = row_ids.iter().map(|s| *s as u32).collect();

    delete_row_ids(&index_path, &row_ids)
}

/// 提交索引修改
pub fn ffi_index_writer_commit(index_path: &CxxString) -> Result<bool, TantivySearchError> {
    let index_path: String = CXX_STRING_CONERTER.convert(index_path).map_err(|e| {
        ERROR!(function: "ffi_index_writer_commit", "Can't convert 'index_path', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;
    commit_index(&index_path)
}

/// 释放索引
pub fn ffi_index_writer_free(index_path: &CxxString) -> Result<bool, TantivySearchError> {
    let index_path: String = CXX_STRING_CONERTER.convert(index_path).map_err(|e| {
        ERROR!(function: "ffi_index_writer_free", "Can't convert 'index_path', message: {}", e);
        TantivySearchError::CxxConvertError(e)
    })?;
    free_index_writer(&index_path)
}
