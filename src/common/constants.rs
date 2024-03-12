use crate::common::flurry_cache::FlurryCache;
use crate::index::bridge::index_writer_bridge_cache::IndexWriterBridgeCache;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::bridge::index_reader_bridge_cache::IndexReaderBridgeCache;
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

// Cache Cxx Converter.
// 转换 CxxString 到 String 类型
pub static CXX_STRING_CONERTER: Lazy<Converter<CxxString, String, CxxElementStrategy>> =
    Lazy::new(|| Converter::new(CxxElementStrategy));

pub static CXX_VECTOR_STRING_CONERTER: Lazy<
    Converter<CxxVector<CxxString>, Vec<String>, CxxVectorStringStrategy>,
> = Lazy::new(|| Converter::new(CxxVectorStringStrategy));

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

// pub fn convert_cxx_string(
//     function: &str,
//     parameter: &str,
//     cxx_string: &CxxString,
// ) -> Result<String, Utf8Error> {
//     let result: Result<String, Utf8Error> = cxx_string.to_str().map(|t| t.to_string());
//     // exception handle
//     if result.is_err() {
//         let exp = format!(
//             "Can't convert `{}`: &CxxString to rust String, exception: {}",
//             parameter,
//             result.err().unwrap()
//         );
//         ERROR!(function: function, "{}", exp);
//     }
//     result
// }

// pub fn convert_cxx_string_vector_to_string_collection<C>(
//     function: &str,
//     parameter: &str,
//     value: &CxxVector<CxxString>,
// ) -> Result<C, Utf8Error>
// where
//     C: FromIterator<String>,
// {
//     let result: Result<_, Utf8Error> = value
//         .iter()
//         .map(|s| s.to_str().map(|t| t.to_string()))
//         .collect();
//     // exception handle
//     if result.is_err() {
//         let exp = format!(
//             "Can't convert `{}`: &CxxVector<CxxString> to rust collector, exception: {}",
//             parameter,
//             result.err().unwrap()
//         );
//         ERROR!(function: function, "{}", exp);
//     }
//     result
// }

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
    // use super::*;
    // use cxx::{let_cxx_string, CxxString};

    // #[test]
    // fn test_convert_cxx_string_success() {
    //     let function_name = "test_function";
    //     let parameter_name = "test_param";
    //     let_cxx_string!(content = "Hello, world!");
    //     let content_cxx: &CxxString = content.as_ref().get_ref();
    //     let result = convert_cxx_string(function_name, parameter_name, &content_cxx);
    //     assert_eq!(result, Ok(content.to_string()));
    // }
}
