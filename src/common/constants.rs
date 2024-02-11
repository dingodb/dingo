use crate::common::flurry_cache::FlurryCache;
use crate::index::ffi_index_writer_cache::FFiIndexWriterCache;
use libc::*;
use once_cell::sync::Lazy;
use once_cell::sync::OnceCell;
use roaring::RoaringBitmap;
use std::sync::Arc;

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
pub extern "C" fn empty_log_callback(_level: i32, _msg: *const c_char, _file: *const c_char) {
    // do nothing
}

// Log callback function lazy init.
pub static LOG_CALLBACK: OnceCell<LogCallback> = OnceCell::new();

// Cache store FFiIndexWriterCache.
pub static FFI_INDEX_WRITER_CACHE: Lazy<FFiIndexWriterCache> = Lazy::new(|| FFiIndexWriterCache::new());

// Cache store FFiIndexSearcherCache.