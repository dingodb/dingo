use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Read, Write};
use std::{fs, path::Path};

use crate::logger::logger_bridge::TantivySearchLogger;

use crate::common::constants::CUSTOM_INDEX_SETTING_FILE_NAME;
use crate::{common::constants::LOG_CALLBACK, WARNING};

/// `CustomIndexSetting` is used to record some custom configuration information about the index,
/// such as the tokenizer and tokenizer parameters.
/// As requirements change, this structure will be further enriched.
/// However, the issue of upgrading to new features needs to be considered.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct CustomIndexSetting {
    #[serde(default = "default_tokenizer")]
    pub tokenizer: String,
    // Others settings.
}

impl Default for CustomIndexSetting {
    fn default() -> Self {
        Self {
            tokenizer: "default".to_string(),
        }
    }
}
fn default_tokenizer() -> String {
    "default".to_string()
}
/// `IndexUtils` serves as a collection of utility functions for index operations.
/// It encapsulates global functions related to managing index directory.
pub struct IndexUtils;

impl IndexUtils {
    /// Before build index, we need ensure this directory is empty.
    pub fn initialize_index_directory(path: &Path) -> Result<(), String> {
        if path.exists() {
            WARNING!(
                "Directory not empty, will remove old data to create new index in this directory:{:?}",
                path
            );

            if let Err(e) = fs::remove_dir_all(path) {
                return Err(format!(
                    "Can't remove directory: {:?}, exception:{}",
                    path,
                    e.to_string()
                ));
            }
        };

        if let Err(e) = fs::create_dir_all(path) {
            return Err(format!(
                "Can't create directory: {:?}, exception:{}",
                path,
                e.to_string()
            ));
        };
        Ok(())
    }

    /// Save the custom index settings to a file.
    pub fn save_custom_index_setting(
        path: &Path,
        setting: &CustomIndexSetting,
    ) -> Result<(), String> {
        let file_path = path.join(CUSTOM_INDEX_SETTING_FILE_NAME);
        let mut file = match File::create(&file_path) {
            Ok(content) => content,
            Err(e) => {
                return Err(format!(
                    "Can't create index setting file:{:?}, exception:{}",
                    path,
                    e.to_string()
                ))
            }
        };

        let setting_json = match serde_json::to_string(setting) {
            Ok(json) => json,
            Err(e) => {
                return Err(format!(
                    "Failed to serialize settings: {}, exception: {}",
                    file_path.display(),
                    e
                ));
            }
        };

        if let Err(e) = file.write_all(setting_json.as_bytes()) {
            return Err(format!(
                "Failed to write custom_index_setting to file: {}, exception: {}",
                file_path.display(),
                e
            ));
        }
        Ok(())
    }

    /// Loads the custom index settings from a file.
    pub fn load_custom_index_setting(index_file_path: &Path) -> Result<CustomIndexSetting, String> {
        let file_path = index_file_path.join(CUSTOM_INDEX_SETTING_FILE_NAME);
        // check whether file exist.
        if !file_path.exists() {
            return Ok(CustomIndexSetting::default());
        }
        let mut file = File::open(file_path.clone()).map_err(|e| {
            format!(
                "Can't open custom index setting file:{:?}, exception:{}",
                file_path,
                e.to_string()
            )
        })?;
        let mut contents = String::new();
        file.read_to_string(&mut contents).map_err(|e| {
            format!(
                "Can't read custom index setting file:{:?}, exception:{}",
                file_path, e
            )
        })?;

        let result: CustomIndexSetting = serde_json::from_str(&contents).map_err(|e| {
            format!(
                "Can't get CustomIndexSetting variable from file:{:?}, exception:{}",
                file_path, e
            )
        })?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        common::index_utils::{CustomIndexSetting, IndexUtils},
        CUSTOM_INDEX_SETTING_FILE_NAME,
    };
    use serde::{Deserialize, Serialize};
    use std::fs::{self, File};
    use tempfile::TempDir;

    #[derive(Serialize, Deserialize)]
    struct CustomIndexSettingV2 {
        tokenizer: String,
        version: u8,
    }

    #[derive(Serialize, Deserialize)]
    struct CustomIndexSettingV3 {
        column: String,
    }

    #[test]
    fn test_initialize_index_directory() {
        let temp_dir = TempDir::new().unwrap();
        let temp_files = vec!["temp1.txt", "temp2.txt"];
        for temp_file in temp_files {
            let file_path = temp_dir.path().join(temp_file);
            let _ = File::create(file_path).unwrap();
        }
        // Currently, this directory contains two files.
        let entries = fs::read_dir(temp_dir.path()).unwrap();
        let old_file_count = entries
            .filter_map(Result::ok)
            .filter(|e| e.path().is_file())
            .count();
        assert_eq!(old_file_count, 2);

        let _ = IndexUtils::initialize_index_directory(temp_dir.path());

        // After initialize index directory, the directory has been cleaned.
        let entries2 = fs::read_dir(temp_dir.path()).unwrap();
        let new_file_count = entries2
            .filter_map(Result::ok)
            .filter(|e| e.path().is_file())
            .count();
        assert_eq!(new_file_count, 0);
    }

    #[test]
    fn test_initialize_index_directory_boundary() {
        // Initialize a not exists directory.
        let temp_dir = TempDir::new().unwrap();
        assert_eq!(temp_dir.path().exists(), true);
        fs::remove_dir_all(temp_dir.path()).unwrap();
        assert_eq!(temp_dir.path().exists(), false);
        let _ = IndexUtils::initialize_index_directory(temp_dir.path());
        assert_eq!(temp_dir.path().exists(), true);
    }

    #[test]
    fn test_save_and_load_custom_index_setting() {
        let temp_dir = TempDir::new().unwrap();
        let custom_setting = CustomIndexSetting {
            tokenizer: "default".to_string(),
        };
        let result = IndexUtils::save_custom_index_setting(temp_dir.path(), &custom_setting);
        assert!(result.is_ok());
        let saved_file_path = temp_dir.path().join(CUSTOM_INDEX_SETTING_FILE_NAME);
        assert!(saved_file_path.exists());

        let result = IndexUtils::load_custom_index_setting(temp_dir.path()).unwrap();
        assert_eq!(result.tokenizer, custom_setting.tokenizer);
    }

    #[test]
    fn test_save_custom_index_setting_boundary() {
        // Trying to save custom index setting in a not exist directory.
        let temp_dir = TempDir::new().unwrap();
        fs::remove_dir_all(temp_dir.path()).unwrap();
        assert_eq!(temp_dir.path().exists(), false);
        let custom_setting = CustomIndexSetting {
            tokenizer: "default".to_string(),
        };
        let result = IndexUtils::save_custom_index_setting(temp_dir.path(), &custom_setting);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_custom_index_setting() {
        let temp_dir = TempDir::new().unwrap();
        let custom_setting = CustomIndexSetting {
            tokenizer: "default".to_string(),
        };

        // Save a custom setting.
        let result = IndexUtils::save_custom_index_setting(&temp_dir.path(), &custom_setting);
        assert!(result.is_ok());

        let loaded_setting = IndexUtils::load_custom_index_setting(temp_dir.path()).unwrap();
        assert_eq!(loaded_setting, custom_setting);
    }

    #[test]
    fn test_load_custom_index_setting_boundary() {
        // Load index settings with an empty directory.
        let temp_dir = TempDir::new().unwrap();
        let result = IndexUtils::load_custom_index_setting(temp_dir.path());
        assert!(result.is_ok());
        assert_eq!(result.unwrap().tokenizer, "default");
    }

    #[test]
    fn test_load_custom_index_setting_version_upgrade() {
        let temp_dir = TempDir::new().unwrap();
        // Create a version V2 `CustomIndexSetting` and serialize it to directory.
        let file_data = CustomIndexSettingV2 {
            tokenizer: "chinese".to_string(),
            version: 2,
        };
        let serialized_person = serde_json::to_string_pretty(&file_data).unwrap();
        let file_path = temp_dir.path().join(CUSTOM_INDEX_SETTING_FILE_NAME);
        fs::write(file_path, serialized_person).unwrap();

        let result = IndexUtils::load_custom_index_setting(temp_dir.path()).unwrap();
        assert_eq!(result.tokenizer, "chinese");
    }

    #[test]
    fn test_load_custom_index_setting_version_downgrade() {
        let temp_dir = TempDir::new().unwrap();
        // Create a version V3 `CustomIndexSetting` and serialize it to directory.
        let file_data = CustomIndexSettingV3 {
            column: "text".to_string(),
        };
        let serialized_person = serde_json::to_string_pretty(&file_data).unwrap();
        let file_path = temp_dir.path().join(CUSTOM_INDEX_SETTING_FILE_NAME);
        fs::write(file_path, serialized_person).unwrap();

        let result = IndexUtils::load_custom_index_setting(temp_dir.path()).unwrap();
        assert_eq!(result.tokenizer, "default");
    }
}
