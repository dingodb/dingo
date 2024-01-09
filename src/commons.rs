use crate::flurry_cache::FlurryCache;
use libc::*;
use once_cell::sync::Lazy;
use once_cell::sync::OnceCell;
use roaring::RoaringBitmap;
use std::sync::Arc;

//**************************** SKIP_INDEX Furry Cache begin *******************************//
// #[cfg(feature = "use-flurry-cache")]
pub static CACHE_FOR_SKIP_INDEX: Lazy<FlurryCache<(usize, String, String), Arc<RoaringBitmap>>> =
    Lazy::new(|| FlurryCache::with_capacity(1000));
//**************************** SKIP_INDEX Furry Cache end *******************************//

//******************************** common variables begin ***********************************//
pub static CUSTOM_INDEX_SETTING_FILE_NAME: &str = "custom_index_setting.json";
//******************************** common variables end ***********************************//

// Store callback function from caller.
pub type LogCallback = extern "C" fn(i32, *const c_char, *const c_char, *const c_char);
pub static LOG_CALLBACK: OnceCell<LogCallback> = OnceCell::new();
