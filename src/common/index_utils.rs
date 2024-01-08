use std::fs::File;
use std::io::{Write, Read};
use std::{path::Path, fs};
use serde::{Serialize, Deserialize};

use crate::commons::CUSTOM_INDEX_SETTING_FILE_NAME;
use crate::logger::ffi_logger::callback_with_thread_info;

use crate::{WARNING, commons::LOG_CALLBACK};


#[derive(Serialize, Deserialize)]
pub struct CustomIndexSetting {
    pub tokenizer: String,
}

/// Before build index, we need prepare this directory.
pub fn prepare_index_directory(path: &Path) -> Result<(), String> {
    if path.exists() {
        WARNING!("Directory not empty, will remove old data to create new index in this directory:{:?}", path);

        if let Err(e) = fs::remove_dir_all(path) {
            return Err(format!("Can't remove directory: {:?}, exception:{}", path, e.to_string()))
        }
    };

    if let Err(e) = fs::create_dir_all(path) {
        return Err(format!("Can't create directory: {:?}, exception:{}", path, e.to_string()))
    };
    Ok(())
}

/// Save the custom index settings to a file.
pub fn save_custom_index_setting(path: &Path, setting: &CustomIndexSetting) -> Result<(), String> {
    let file_path = path.join(CUSTOM_INDEX_SETTING_FILE_NAME);
    let mut file = match File::create(&file_path) {
        Ok(content) => content,
        Err(e) => {
            return Err(format!("Can't create index setting file:{:?}, exception:{}", path, e.to_string()))
        }
    };

    let setting_json = match serde_json::to_string(setting) {
        Ok(json) => json,
        Err(e) => {
            return Err(format!("Failed to serialize settings: {}, exception: {}", file_path.display(), e));
        }
    };

    if let Err(e) = file.write_all(setting_json.as_bytes()) {
        return Err(format!("Failed to write custom_index_setting to file: {}, exception: {}", file_path.display(), e));
    }
    Ok(())
}

/// Loads the custom index settings from a file.
pub fn load_custom_index_setting(index_file_path: &Path) -> Result<CustomIndexSetting, String> {
    let file_path = index_file_path.join(CUSTOM_INDEX_SETTING_FILE_NAME);
    let mut file = match File::open(file_path.clone()) {
        Ok(content) => content,
        Err(e) => {
            return Err(format!("Can't open custom index setting file:{:?}, exception:{}", file_path, e.to_string()))
        }
    };
    let mut contents = String::new();
    if let Err(e) = file.read_to_string(&mut contents) {
        return Err(format!("Can't read custom index setting file:{:?}, exception:{}", file_path, e.to_string()))
    };
    let result: CustomIndexSetting = match serde_json::from_str(&contents) {
        Ok(content) => content,
        Err(e)=>{
            return Err(format!("Can't get CustomIndexSetting variable from file:{:?}, exception:{}", file_path, e.to_string()))
        }
    };
    Ok(result)
}


#[cfg(test)]
mod tests {
    #[test]
    fn test_prepare_index_directory(){
        // prepare_index_directory()
    }
}