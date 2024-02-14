use crate::common::constants::*;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::ERROR;
use libc::*;
use std::ffi::CStr;

use super::logger_config::LoggerConfig;

#[no_mangle]
pub extern "C" fn tantivy_search_log4rs_initialize(
    log_directory: *const c_char,
    log_level: *const c_char,
    log_in_file: bool,
    console_dispaly: bool,
    only_record_tantivy_search: bool,
) -> bool {
    tantivy_search_log4rs_initialize_with_callback(
        log_directory,
        log_level,
        log_in_file,
        console_dispaly,
        only_record_tantivy_search,
        empty_log_callback,
    )
}
/// Initializes the logger configuration for the tantivy search library.
///
/// Arguments:
/// - `log_path`: The path where log files are saved. Tantivy-search will generate multiple log files.
/// - `log_level`: The logging level to use. Supported levels: info, debug, trace, error, warning.
/// - `log_in_file`: Whether record log content in file.
///   Note: 'fatal' is treated as 'error'.
/// - `console_dispaly`: Enables logging to the console if set to true.
/// - `only_tantivy_search`: Only record `target=tantivy_search` log content.
/// - `callback`: A callback function, typically provided by ClickHouse.
///
/// Returns:
/// - `true` if the logger is successfully initialized, `false` otherwise.
#[no_mangle]
pub extern "C" fn tantivy_search_log4rs_initialize_with_callback(
    log_directory: *const c_char,
    log_level: *const c_char,
    log_in_file: bool,
    console_dispaly: bool,
    only_record_tantivy_search: bool,
    callback: LogCallback,
) -> bool {
    if log_directory.is_null() || log_level.is_null() {
        ERROR!("`log_directory` or `log_level` can't be nullptr");
        return false;
    }
    // Safely convert C strings to Rust String, checking for null pointers.
    let log_directory: String = match unsafe { CStr::from_ptr(log_directory) }.to_str() {
        Ok(path) => path.to_owned(),
        Err(_) => {
            ERROR!("`log_directory` (string) is invalid");
            return false;
        }
    };
    let log_level: String = match unsafe { CStr::from_ptr(log_level) }.to_str() {
        Ok(level) => level.to_owned(),
        Err(_) => {
            ERROR!("`log_level` (string) is invalid");
            return false;
        }
    };

    let logger_config = LoggerConfig::new(
        log_directory.clone(),
        log_level.clone(),
        log_in_file,
        console_dispaly,
        only_record_tantivy_search,
    );

    match TantivySearchLogger::update_log_callback(&LOG_CALLBACK, callback) {
        Ok(_) => {}
        Err(e) => {
            ERROR!("{:?}", e);
            return false;
        }
    };
    let config = match logger_config.build_logger_config() {
        Ok(config) => config,
        Err(e) => {
            ERROR!("{:?}", e);
            return false;
        }
    };
    match TantivySearchLogger::update_log4rs_handler(&LOG4RS_HANDLE, config) {
        Ok(_) => {}
        Err(e) => {
            ERROR!("{:?}", e);
            return false;
        }
    };

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    // Convert a Rust string to a C-style string.
    fn to_c_str(s: &str) -> *const c_char {
        CString::new(s).unwrap().into_raw()
    }

    #[test]
    fn test_tantivy_search_log4rs_initialize_null_arguments() {
        // Test with null log_directory and log_level to ensure it returns false.
        assert_eq!(
            tantivy_search_log4rs_initialize(
                std::ptr::null(),
                std::ptr::null(),
                false,
                false,
                false
            ),
            false
        );
    }

    #[test]
    fn test_tantivy_search_log4rs_initialize_invalid_path() {
        // Assuming the function checks for the validity of the path,
        let invalid_path = to_c_str(""); // will trigger a permission denied error.
        let log_level = to_c_str("info");
        assert_eq!(
            tantivy_search_log4rs_initialize(invalid_path, log_level, true, true, false),
            false
        );
    }

    #[test]
    fn test_tantivy_search_log4rs_initialize_invalid_log_level() {
        // Provide an invalid log level and expect true, cause it will be treated as info.
        let log_directory = to_c_str("/tmp");
        let invalid_log_level = to_c_str("invalid"); // Assuming this is an invalid log level.
        assert_eq!(
            tantivy_search_log4rs_initialize(log_directory, invalid_log_level, true, true, false),
            true
        );
    }
}
