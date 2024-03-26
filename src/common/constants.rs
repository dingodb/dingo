use crate::index::bridge::index_writer_bridge_cache::IndexWriterBridgeCache;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::bridge::index_reader_bridge_cache::IndexReaderBridgeCache;
use crate::common::cache::flurry_cache::FlurryCache;
use cxx::vector::VectorElement;
use cxx::CxxString;
use cxx::CxxVector;
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

use super::converter::Converter;
use super::converter::CxxElementStrategy;
use super::converter::CxxVectorStrategy;
use super::converter::CxxVectorStringStrategy;

// Cache queries results.
// The cache's key is composed of reader.address, query_str, index_directory, use_regex.
#[allow(dead_code)]
pub static CACHE_FOR_SKIP_INDEX: Lazy<
    FlurryCache<(usize, String, String, bool), Arc<RoaringBitmap>>,
> = Lazy::new(|| FlurryCache::with_capacity(1000));

// Custom index settings file name.
pub static INDEX_INFO_FILE_NAME: &str = "custom_index_setting.json";

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

/// Convert 'CxxString' to 'String'
pub static CXX_STRING_CONERTER: Lazy<Converter<CxxString, String, CxxElementStrategy>> =
    Lazy::new(|| Converter::new(CxxElementStrategy));

/// Convert 'CxxVector<CxxString>' to 'Vec<String>'
pub static CXX_VECTOR_STRING_CONERTER: Lazy<
    Converter<CxxVector<CxxString>, Vec<String>, CxxVectorStringStrategy>,
> = Lazy::new(|| Converter::new(CxxVectorStringStrategy));

/// Convert 'CxxVector<T> to Vec<T>'
pub fn cxx_vector_converter<T>() -> Converter<CxxVector<T>, Vec<T>, CxxVectorStrategy<T>>
where
    T: Clone + VectorElement,
{
    Converter::new(CxxVectorStrategy::new())
}

// language filters
// pub static stop_word_filters: Vec<String> = ["".to_string()].to_vec();
// Avoid some unit test run concurrently.
#[allow(dead_code)]
pub static TEST_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

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
