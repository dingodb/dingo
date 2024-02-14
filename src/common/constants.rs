use crate::common::flurry_cache::FlurryCache;
use crate::index::bridge::index_writer_bridge_cache::IndexWriterBridgeCache;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::bridge::index_reader_bridge_cache::IndexReaderBridgeCache;
use crate::ERROR;
use cxx::CxxString;
use libc::*;
use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::config::Appender;
use log4rs::config::Root;
use log4rs::encode::pattern::PatternEncoder;
use log4rs::Config;
use once_cell::sync::Lazy;
use once_cell::sync::OnceCell;
use roaring::RoaringBitmap;
use std::sync::Arc;
use std::sync::Mutex;

// Cache queries results.
// The cache's key is composed of reader.address, query_str, index_directory, use_regex.
pub static CACHE_FOR_SKIP_INDEX: Lazy<
    FlurryCache<(usize, String, String, bool), Arc<RoaringBitmap>>,
> = Lazy::new(|| FlurryCache::with_capacity(1000));

// Custom index settings file name.
pub static CUSTOM_INDEX_SETTING_FILE_NAME: &str = "custom_index_setting.json";

// Log callback function type.
pub type LogCallback = extern "C" fn(i32, *const c_char, *const c_char);

// Empty log callback.
pub extern "C" fn empty_log_callback(_level: i32, _info: *const c_char, _message: *const c_char) {
    // do nothing
}
// Log4rs handler, related with logger.
pub static LOG4RS_HANDLE: OnceCell<log4rs::Handle> = OnceCell::new();
// Log callback function lazy init.
pub static LOG_CALLBACK: OnceCell<LogCallback> = OnceCell::new();

// Cache store IndexWriterBridgeCache.
pub static FFI_INDEX_WRITER_CACHE: Lazy<IndexWriterBridgeCache> =
    Lazy::new(|| IndexWriterBridgeCache::new());

// Cache store IndexReaderBridgeCache.
pub static FFI_INDEX_SEARCHER_CACHE: Lazy<IndexReaderBridgeCache> =
    Lazy::new(|| IndexReaderBridgeCache::new());

// Avoid some unit test run concurrently.
#[allow(dead_code)]
pub static TEST_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

pub fn convert_cxx_string(
    function: &str,
    parameter: &str,
    cxx_string: &CxxString,
) -> Result<String, String> {
    let cxx_converted = match cxx_string.to_str() {
        Ok(content) => content.to_string(),
        Err(e) => {
            let exp = format!(
                "Can't convert cxx_string `{}`:[{}], exception: {}",
                parameter,
                cxx_string,
                e.to_string()
            );
            ERROR!(function: function, "{}", exp);
            return Err(exp);
        }
    };
    Ok(cxx_converted)
}

#[allow(dead_code)]
pub fn update_logger_for_test(level: LevelFilter) {
    let stdout_appender = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} - {l} - {m}\n")))
        .build();

    let log_config_info = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout_appender)))
        .build(Root::builder().appender("stdout").build(level))
        .expect("Failed to build log config with stdout appender");

    // assert!(LOG4RS_HANDLE.get().is_none());
    let result = TantivySearchLogger::update_log4rs_handler(&LOG4RS_HANDLE, log_config_info);
    assert!(result.is_ok());
}

#[cfg(test)]
mod tests {
    use super::*;
    use cxx::{let_cxx_string, CxxString};

    #[test]
    fn test_convert_cxx_string_success() {
        let function_name = "test_function";
        let parameter_name = "test_param";
        let_cxx_string!(content = "Hello, world!");
        let content_cxx: &CxxString = content.as_ref().get_ref();
        let result = convert_cxx_string(function_name, parameter_name, &content_cxx);
        assert_eq!(result, Ok(content.to_string()));
    }
}
