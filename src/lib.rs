use libc::*;
use std::ffi::CStr;
use std::ffi::c_uint;

mod commons;
mod flurry_cache;
mod logger;
mod tokenizer;
mod utils;
pub mod index;
pub mod search;
mod common;


use commons::*;
use logger::ffi_logger::*;
use index::index_manager::*;
use search::index_searcher::*;



#[cxx::bridge(namespace="TANTIVY")]
mod ffi {
    extern "Rust" {
        // fn tantivy_logger_initialize(log_path: &CxxString, log_level: &CxxString, console_logging: bool, callback: LogCallback, enable_callback: bool, only_tantivy_search: bool) -> Result<bool>;
        fn tantivy_create_index_with_tokenizer(index_path: &CxxString, tokenizer_with_parameter: &CxxString) -> Result<bool>;
        fn tantivy_create_index(index_path: &CxxString) -> Result<bool>;
        fn tantivy_load_index(index_path: &CxxString) -> Result<bool>;
        fn tantivy_index_doc(index_path: &CxxString, row_id: u64, doc: &CxxString) -> Result<bool>;
        fn tantivy_writer_commit(index_path: &CxxString) -> Result<bool>;
        fn tantivy_reader_free(index_path: &CxxString) -> Result<bool>;
        fn tantivy_writer_free(index_path: &CxxString) -> Result<bool>;
        fn tantivy_search_in_rowid_range(index_path: &CxxString, query: &CxxString, lrange: u64, rrange: u64, use_regex: bool) -> Result<bool>;
        fn tantivy_count_in_rowid_range(index_path: &CxxString, query: &CxxString, lrange: u64, rrange: u64, use_regex: bool) -> Result<u64>;
    }
}

pub type LogCallback = extern "C" fn(i32, *const c_char, *const c_char, *const c_char);


/// Initializes the logger configuration for the tantivy search library.
///
/// Arguments:
/// - `log_path`: The path where log files are saved. Tantivy-search will generate multiple log files.
/// - `log_level`: The logging level to use. Supported levels: info, debug, trace, error, warning.
///   Note: 'fatal' is treated as 'error'.
/// - `console_logging`: Enables logging to the console if set to true.
/// - `callback`: A callback function, typically provided by ClickHouse.
/// - `enable_callback`: Enables the use of the callback function if set to true.
/// - `only_tantivy_search`: Only display `tantivy_search` log content.
///
/// Returns:
/// - `true` if the logger is successfully initialized, `false` otherwise.
#[no_mangle]
pub extern "C" fn tantivy_logger_initialize(
    log_path: *const c_char,
    log_level: *const c_char,
    console_logging: bool,
    callback: LogCallback,
    enable_callback: bool,
    only_tantivy_search: bool,
) -> bool {
    // Safely convert C strings to Rust String, checking for null pointers.
    let log_path = match unsafe { CStr::from_ptr(log_path) }.to_str() {
        Ok(path) => path.to_owned(),
        Err(_) => {
            ERROR!("Log path (string) can't be null or invalid");
            return false;
        }
    };
    let log_level = match unsafe { CStr::from_ptr(log_level) }.to_str() {
        Ok(level) => level.to_owned(),
        Err(_) => {
            ERROR!("Log level (string) can't be null or invalid");
            return false;
        }
    };

    match initialize_tantivy_search_logger(
        log_path,
        log_level,
        console_logging,
        callback,
        enable_callback,
        only_tantivy_search,
    ) {
        Ok(_) => true,
        Err(e) => {
            ERROR!("Can't config logger. {}", e);
            false
        }
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
