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
