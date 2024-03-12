use crate::common::constants::INDEX_INFO_FILE_NAME;
use crate::common::errors::IndexUtilsError;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::tokenizer::dto::index_parameter_dto::IndexParameterDTO;
use crate::{common::constants::LOG_CALLBACK, WARNING};
use std::fs::File;
use std::io::{Read, Write};
use std::{fs, path::Path};

/// `IndexUtils` serves as a collection of utility functions for index operations.
/// It encapsulates global functions related to managing index directory.
pub struct IndexUtils;

impl IndexUtils {
    /// Before build index, we need ensure this directory is empty.
    pub fn initialize_index_directory(path: &Path) -> Result<(), IndexUtilsError> {
        if path.exists() {
            WARNING!("Directory not empty, will recreate: {:?}", path);
            fs::remove_dir_all(path).map_err(|e| {
                IndexUtilsError::RemoveDirectoryError(format!(
                    "path: {:?}, message: {}",
                    path,
                    e.to_string()
                ))
            })?;
        };

        fs::create_dir_all(path).map_err(|e| {
            IndexUtilsError::CreateDirectoryError(format!(
                "path: {:?}, message: {}",
                path,
                e.to_string()
            ))
        })?;
        Ok(())
    }

    /// Save the custom index settings to a file.
    pub fn save_custom_index_setting(
        path: &Path,
        setting: &IndexParameterDTO,
    ) -> Result<(), IndexUtilsError> {
        let file_path = path.join(INDEX_INFO_FILE_NAME);
        let mut file = File::create(&file_path).map_err(|e| {
            IndexUtilsError::WriteFileError(format!(
                "file: {:?}, message: {}",
                file_path,
                e.to_string()
            ))
        })?;

        let setting_json = serde_json::to_string(setting).map_err(|e| {
            IndexUtilsError::JsonSerializeError(format!(
                "file: {:?}, message: {}",
                file_path,
                e.to_string()
            ))
        })?;

        if let Err(e) = file.write_all(setting_json.as_bytes()) {
            return Err(IndexUtilsError::WriteFileError(format!(
                "file: {:?}, message: {}",
                file_path,
                e.to_string()
            )));
        }
        Ok(())
    }

    /// Loads the custom index settings from a file.
    pub fn load_custom_index_setting(
        index_file_path: &Path,
    ) -> Result<IndexParameterDTO, IndexUtilsError> {
        let file_path = index_file_path.join(INDEX_INFO_FILE_NAME);
        // check whether file exist.
        if !file_path.exists() {
            return Ok(IndexParameterDTO::default());
        }
        let mut file = File::open(file_path.clone()).map_err(|e| {
            IndexUtilsError::ReadFileError(format!(
                "file: {:?}, message: {}",
                file_path,
                e.to_string()
            ))
        })?;
        let mut contents = String::new();
        file.read_to_string(&mut contents).map_err(|e| {
            IndexUtilsError::ReadFileError(format!(
                "file: {:?}, message: {}",
                file_path,
                e.to_string()
            ))
        })?;

        let result: IndexParameterDTO = serde_json::from_str(&contents).map_err(|e| {
            IndexUtilsError::JsonDeserializeError(format!(
                "file: {:?}, message: {}",
                file_path,
                e.to_string()
            ))
        })?;
        Ok(result)
    }
}
