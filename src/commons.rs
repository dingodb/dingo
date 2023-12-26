use crate::furry_cache::FurryCache;
use once_cell::sync::Lazy;
use roaring::RoaringBitmap;
use std::sync::Arc;
use once_cell::sync::OnceCell;
use libc::*;

//****************************** FFI function names begin *********************************//
pub static LOGGER_TARGET: &str = "tantivy_search"; // default target is tantivy_search.
pub static TANTIVY_LOGGER_INITIALIZE_NAME: &str = "tantivy_logger_initialize";
pub static TANTIVY_CREATE_INDEX_WITH_LANGUAGE_NAME: &str = "tantivy_create_index_with_language";
pub static TANTIVY_CREATE_INDEX_NAME: &str = "tantivy_create_index";
pub static TANTIVY_LOAD_INDEX_NAME: &str = "tantivy_load_index";
pub static TANTIVY_SEARCH_IN_ROWID_RANGE_NAME: &str = "tantivy_search_in_rowid_range";
pub static TANTIVY_COUNT_IN_ROWID_RANGE_NAME: &str = "tantivy_count_in_rowid_range";
pub static TANTIVY_INDEX_DOC_NAME: &str = "tantivy_index_doc";
pub static TANTIVY_WRITER_COMMIT_NAME: &str = "tantivy_writer_commit";
pub static TANTIVY_READER_FREE_NAME: &str = "tantivy_reader_free";
pub static TANTIVY_WRITER_FREE_NAME: &str = "tantivy_writer_free";
//****************************** FFI function names end *********************************//

//**************************** SKIP_INDEX Furry Cache begin *******************************//
#[cfg(feature = "use-flurry-cache")]
pub static CACHE_FOR_SKIP_INDEX: Lazy<FurryCache<(usize, String, String), Arc<RoaringBitmap>>> =
    Lazy::new(|| FurryCache::with_capacity(1000));
//**************************** SKIP_INDEX Furry Cache end *******************************//

//******************************** common variables begin ***********************************//
pub static CUSTOM_INDEX_SETTING_FILE_NAME: &str = "custom_index_setting.json";
//******************************** common variables end ***********************************//

// Store callback function from caller.
pub type LogCallback = extern "C" fn(i32, *const c_char, *const c_char, *const c_char);
pub static LOG_CALLBACK: OnceCell<LogCallback> = OnceCell::new();
