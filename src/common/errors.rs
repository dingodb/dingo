//! Definition of Tantivy's errors and results.

use std::str::Utf8Error;

use tantivy::TantivyError;
use thiserror::Error;

#[derive(Debug, Clone, Error)]
#[allow(dead_code)]
pub enum TokenizerUtilsError {
    #[error("Failed to parse json str. '{0}'")]
    JsonParseError(String),
    #[error("Failed to deserialize json str. '{0}'")]
    JsonDeserializeError(String),
    #[error("Failed to config tokenizer. '{0}'")]
    ConfigTokenizerError(String),
    #[error("Unsupported tokenizer type. '{0}'")]
    UnsupportedTokenizerType(String),
}

#[derive(Debug, Clone, Error)]
#[allow(dead_code)]
pub enum IndexUtilsError {
    #[error("Failed to convert cxx vector variable. '{0}'")]
    JsonParseError(String),
    #[error("Failed to convert cxx vector variable. '{0}'")]
    JsonSerializeError(String),
    #[error("Failed to convert cxx vector variable. '{0}'")]
    JsonDeserializeError(String),
    #[error("Failed to handle directory. '{0}'")]
    DirectoryIOError(String),
    #[error("Failed to config tokenizer. '{0}'")]
    ConfigTokenizerError(String),

    #[error("Failed to remove directory. '{0}'")]
    RemoveDirectoryError(String),
    #[error("Failed to create directory. '{0}'")]
    CreateDirectoryError(String),
    #[error("Failed to read file. '{0}'")]
    ReadFileError(String),
    #[error("Failed to write file. '{0}'")]
    WriteFileError(String),
}
#[derive(Debug, Clone, Error)]
#[allow(dead_code)]
pub enum CxxConvertError {
    #[error("Failed to convert cxx vector variable. '{0}'")]
    CxxVectorConvertError(String),
    #[error("Failed to convert cxx element variable. '{0}'")]
    CxxElementConvertError(String),
    #[error("Failed to convert CxxString to Rust String: {0}")]
    Utf8Error(#[from] Utf8Error),
}

/// The library's error enum
#[derive(Debug, Clone, Error)]
#[allow(dead_code)]
pub enum TantivySearchError {
    #[error(transparent)]
    CxxConvertError(#[from] CxxConvertError),

    #[error(transparent)]
    IndexUtilsError(#[from] IndexUtilsError),

    #[error(transparent)]
    TokenizerUtilsError(#[from] TokenizerUtilsError),

    #[error(transparent)]
    TantivyError(#[from] TantivyError),

    #[error("Index not exists: '{0}'")]
    IndexNotExists(String),

    /// An internal error occurred. This is are internal states that should not be reached.
    /// e.g. a datastructure is incorrectly inititalized.
    #[error("Internal error: '{0}'")]
    InternalError(String),

    #[error("An invalid argument was passed: '{0}'")]
    InvalidArgument(String),
}

// impl From<CxxConvertError> for TantivySearchError {
//     fn from(cxx_convert_error: CxxConvertError) -> TantivySearchError {
//         TantivySearchError::InvalidArgument(format!("Query is invalid. {cxx_convert_error:?}"))
//     }
// }
