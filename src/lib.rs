use libc::*;
use roaring::RoaringBitmap;
use std::ffi::{CStr, CString};
use std::path::Path;
use std::sync::Arc;
use tantivy::schema::{Schema, FAST, INDEXED};
use utils::{CustomIndexSetting, SearchError};
use utils::{IndexR, IndexW};

use tantivy::query::{QueryParser, RegexQuery};
use tantivy::{Document, Index, ReloadPolicy};

mod commons;
mod furry_cache;
pub mod logger;
mod row_id_bitmap_collector;
mod third_party;
pub mod utils;

use commons::*;
use logger::ffi_logger::*;
use row_id_bitmap_collector::RowIdRoaringCollector;
use third_party::third_party_tokenizer::*;

use crate::utils::{load_custom_index_setting, prepare_index_directory, save_custom_index_setting, like_to_regex};


pub fn add2(left: usize, right: usize) -> usize {
    left + right
}
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
            ERROR!(target: LOGGER_TARGET, function: TANTIVY_LOGGER_INITIALIZE_NAME, "Log path (string) can't be null or invalid");
            return false;
        }
    };
    let log_level = match unsafe { CStr::from_ptr(log_level) }.to_str() {
        Ok(level) => level.to_owned(),
        Err(_) => {
            ERROR!(target: LOGGER_TARGET, function: TANTIVY_LOGGER_INITIALIZE_NAME, "Log level (string) can't be null or invalid");
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
            ERROR!(target:LOGGER_TARGET, function: TANTIVY_LOGGER_INITIALIZE_NAME, "Can't config logger. {}", e);
            false
        }
    }
}

/// Creates an index using a specified language (e.g., Chinese, English, Japanese, etc.).
///
/// Arguments:
/// - `dir_ptr`: The directory path for building the index.
/// - `language`: The language to be used.
///
/// Returns:
/// - A pointer to the created `IndexW`, or a null pointer if an error occurs.
#[no_mangle]
pub extern "C" fn tantivy_create_index_with_language(
    dir_ptr: *const c_char,
    language: *const c_char,
) -> *mut IndexW {
    // Convert C strings to Rust strings and handle potential null pointers.
    let dir_str = unsafe {
        if dir_ptr.is_null() {
            ERROR!(target: LOGGER_TARGET, function:TANTIVY_CREATE_INDEX_WITH_LANGUAGE_NAME, "Directory path cannot be null");
            return std::ptr::null_mut();
        }
        match CStr::from_ptr(dir_ptr).to_str() {
            Ok(str) => str.to_owned(),
            Err(_) => {
                ERROR!(target: LOGGER_TARGET, function:TANTIVY_CREATE_INDEX_WITH_LANGUAGE_NAME, "Invalid directory path");
                return std::ptr::null_mut();
            }
        }
    };

    let language_str = unsafe {
        if language.is_null() {
            ERROR!(target: LOGGER_TARGET, function:TANTIVY_CREATE_INDEX_WITH_LANGUAGE_NAME, "Language cannot be null");
            return std::ptr::null_mut();
        }
        match CStr::from_ptr(language).to_str() {
            Ok(str) => str.to_owned(),
            Err(_) => {
                ERROR!(target: LOGGER_TARGET, function:TANTIVY_CREATE_INDEX_WITH_LANGUAGE_NAME, "Invalid language string");
                return std::ptr::null_mut();
            }
        }
    };

    // Prepare the index directory for use.
    let index_file_path = Path::new(&dir_str);
    if let Err(e) = prepare_index_directory(&index_file_path) {
        ERROR!(target: LOGGER_TARGET, function:TANTIVY_CREATE_INDEX_WITH_LANGUAGE_NAME, "Error preparing index directory: {}", e);
        return std::ptr::null_mut();
    }

    // Save custom index settings.
    let custom_index_setting = CustomIndexSetting {
        language: language_str.clone(),
    };

    if let Err(e) = save_custom_index_setting(&index_file_path, &custom_index_setting) {
        ERROR!(target: LOGGER_TARGET, function:TANTIVY_CREATE_INDEX_WITH_LANGUAGE_NAME, "Error saving custom index settings: {}", e);
        return std::ptr::null_mut();
    }

    INFO!(target: LOGGER_TARGET, function:TANTIVY_CREATE_INDEX_WITH_LANGUAGE_NAME, "Custom index setting has been saved {}", "");

    // Get and register the tokenizer for the specified language.
    let (third_party_tokenizer_name, third_party_tokenizer, third_party_text_options) =
        get_third_party_tokenizer(&language_str);

    // Construct the schema for the index.
    let mut schema_builder = Schema::builder();
    schema_builder.add_u64_field("row_id", FAST | INDEXED);
    schema_builder.add_text_field("text", third_party_text_options);
    let schema = schema_builder.build();

    // Create the index in the specified directory.
    let mut index = match Index::create_in_dir(&dir_str, schema) {
        Ok(index) => index,
        Err(e) => {
            ERROR!(target: LOGGER_TARGET, function:TANTIVY_CREATE_INDEX_WITH_LANGUAGE_NAME, "Failed to create index in directory:{}; exception:{}", dir_str, e);
            return std::ptr::null_mut();
        }
    };

    // Register the tokenizer with the index.
    if let Err(e) = register_tokenizer_to_index(
        &mut index,
        third_party_tokenizer_name.clone(),
        third_party_tokenizer,
    ) {
        WARNING!(target: LOGGER_TARGET, function:TANTIVY_CREATE_INDEX_WITH_LANGUAGE_NAME, "Failed to register tokenizer: {:?}, exception: {}", third_party_tokenizer_name, e);
    }

    // Create the writer with a specified buffer size (e.g., 1 GB).
    let writer = match index.writer_with_num_threads(2, 1024 * 1024 * 64) {
        // 1 GB
        Ok(w) => w,
        Err(e) => {
            ERROR!(target: LOGGER_TARGET, function:TANTIVY_CREATE_INDEX_WITH_LANGUAGE_NAME, "Failed to create tantivy writer: {}", e);
            return std::ptr::null_mut();
        }
    };

    // Configure and set the merge policy.
    let mut merge_policy = tantivy::merge_policy::LogMergePolicy::default();
    merge_policy.set_min_num_segments(5); // 设置合并操作中至少应该包含多少个段 // Minimum number of segments to include in a merge.
    merge_policy.set_max_docs_before_merge(500000); // 设置在考虑合并前段可以有的最大文档数量为10w // Maximum document count threshold before merge.
    merge_policy.set_min_layer_size(10000);
    merge_policy.set_level_log_size(0.75);
    // merge_policy.set_min_num_segments(4); // 设置合并操作中至少应该包含多少个段 // Minimum number of segments to include in a merge.
                                          // policy.set_max_merge_size(3_000_000);
    writer.set_merge_policy(Box::new(merge_policy));

    // Convert the Box smart pointer to a raw pointer, avoiding memory deconstruction.
    Box::into_raw(Box::new(IndexW {
        index,
        writer,
        path: dir_str,
    }))
}

/// Creates an index using the default language.
///
/// Arguments:
/// - `dir_ptr`: A pointer to the directory path where the index files will be created.
///
/// Returns:
/// - A pointer to `IndexW`, which encapsulates the index and writer.
/// - Returns a null pointer if any error occurs during the process.
#[no_mangle]
pub extern "C" fn tantivy_create_index(dir_ptr: *const c_char) -> *mut IndexW {
    // Check for null pointer before proceeding.
    if dir_ptr.is_null() {
        // Logging error and returning a null pointer if the directory path is null.
        ERROR!(target: LOGGER_TARGET, function: TANTIVY_CREATE_INDEX_NAME, "Directory path cannot be null");
        return std::ptr::null_mut();
    }

    // Using the "default" tokenizer.
    // Safe conversion from Rust string to C string.
    let tokenizer_name = match CString::new("default") {
        Ok(name) => name,
        Err(e) => {
            // Log the error if CString::new fails.
            ERROR!(target: LOGGER_TARGET, function: TANTIVY_CREATE_INDEX_NAME, "Failed to create CString for default tokenizer: {}", e);
            return std::ptr::null_mut();
        }
    };

    // Delegate to `tantivy_create_index_with_language` using the default tokenizer.
    tantivy_create_index_with_language(dir_ptr, tokenizer_name.as_ptr())
}

/// Loads an index from a specified directory.
///
/// Arguments:
/// - `dir_ptr`: A pointer to the directory path where the index files are located.
///
/// Returns:
/// - A pointer to `IndexR`, which encapsulates the loaded index and reader.
/// - Returns a null pointer if any error occurs during the loading process.
#[no_mangle]
pub extern "C" fn tantivy_load_index(dir_ptr: *const c_char) -> *mut IndexR {
    // Convert C string to Rust string and handle potential null pointers.
    let dir_str = unsafe {
        if dir_ptr.is_null() {
            ERROR!(target: LOGGER_TARGET, function: TANTIVY_LOAD_INDEX_NAME, "Directory path cannot be null");
            return std::ptr::null_mut();
        }
        match CStr::from_ptr(dir_ptr).to_str() {
            Ok(str) => str.to_owned(),
            Err(_) => {
                ERROR!(target: LOGGER_TARGET, function: TANTIVY_LOAD_INDEX_NAME, "Invalid directory path");
                return std::ptr::null_mut();
            }
        }
    };

    let index_file_path = Path::new(&dir_str);
    if !index_file_path.exists() || !index_file_path.is_dir() {
        ERROR!(target: LOGGER_TARGET, function: TANTIVY_LOAD_INDEX_NAME, "Directory does not exist or is not a directory");
        return std::ptr::null_mut();
    }

    // Open the index in the specified directory.
    let mut index = match Index::open_in_dir(index_file_path) {
        Ok(idx) => idx,
        Err(e) => {
            ERROR!(target: LOGGER_TARGET, function: TANTIVY_LOAD_INDEX_NAME, "Failed to open index in directory: {}", e);
            return std::ptr::null_mut();
        }
    };

    // Load custom index settings.
    let custom_index_setting = match load_custom_index_setting(index_file_path) {
        Ok(setting) => setting,
        Err(e) => {
            ERROR!(target: LOGGER_TARGET, function: TANTIVY_LOAD_INDEX_NAME, "Error loading custom index settings: {}", e);
            return std::ptr::null_mut();
        }
    };
    INFO!(target: LOGGER_TARGET, function: TANTIVY_LOAD_INDEX_NAME, "Custom index setting loaded: {}", serde_json::to_string(&custom_index_setting).unwrap_or_default());

    // Register tokenizer based on the loaded settings.
    let (third_party_tokenizer_name, third_party_tokenizer, _) =
        get_third_party_tokenizer(&custom_index_setting.language);
    if let Err(e) = register_tokenizer_to_index(
        &mut index,
        third_party_tokenizer_name.clone(),
        third_party_tokenizer,
    ) {
        WARNING!(target: LOGGER_TARGET, function: TANTIVY_LOAD_INDEX_NAME, "Failed to register tokenizer: {:?}, exception: {}", third_party_tokenizer_name, e);
    }

    // Set the multithreaded executor for search.
    if let Err(e) = index.set_default_multithread_executor() {
        ERROR!(target: LOGGER_TARGET, function: TANTIVY_LOAD_INDEX_NAME, "Failed to set multithread executor: {}", e);
        return std::ptr::null_mut();
    }

    // Create a reader for the index with an appropriate reload policy.
    // OnCommit: reload when commit; Manual: developer need call IndexReader::reload() to reload.
    let reader = match index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()
    {
        Ok(r) => r,
        Err(e) => {
            ERROR!(target: LOGGER_TARGET, function: TANTIVY_LOAD_INDEX_NAME, "Failed to create tantivy reader: {}", e);
            return std::ptr::null_mut();
        }
    };

    // Convert the Box smart pointer to a raw pointer, avoiding memory deconstruction.
    Box::into_raw(Box::new(IndexR {
        index,
        reader,
        path: dir_str,
    }))
}

fn compute_bitmap(indexr: &IndexR, query_str: &str, function_name: &str, use_regrex: bool) -> Arc<RoaringBitmap> {
    // in compute_bitmap, any exception will generate an empty bitmap
    // index 可能已经被释放？使用与 searcher 关联的 index 重新尝试一下
    // let schema = indexr.index.schema();

    let schema = indexr.reader.searcher().index().schema();
    let text = match schema.get_field("text") {
        Ok(str) => str,
        Err(_) => {
            WARNING!(target: LOGGER_TARGET, function: function_name, "Missing text field.");
            return Arc::new(RoaringBitmap::new());
        }
    };
    let searcher = indexr.reader.searcher();
    // index 可能已经被释放？使用与 searcher 关联的 index 重新尝试一下
    // let query_parser = QueryParser::for_index(&indexr.index, vec![text]);

    if  use_regrex  {
        let regex_query = match RegexQuery::from_pattern(&like_to_regex(query_str), text) {
            Ok(parsed_query) => parsed_query,
            Err(e) => {
                WARNING!(target: LOGGER_TARGET, function: function_name, "Can't parse regrex query: {}, {}", like_to_regex(query_str), e);
                return Arc::new(RoaringBitmap::new());
            }
        };
        // TODO 优化这部分重复的代码
        let searched_bitmap = match searcher.search(
            &regex_query,
            &RowIdRoaringCollector::with_field("row_id".to_string()),
        ) {
            Ok(result_bitmap) => result_bitmap,
            Err(e) => {
                WARNING!(target: LOGGER_TARGET, function: function_name, "Can't execute search in text_query: {}", e);
                return Arc::new(RoaringBitmap::new());
            }
        };
        searched_bitmap
    } else {
        let query_parser = QueryParser::for_index(indexr.reader.searcher().index(), vec![text]);
        let text_query = match query_parser.parse_query(query_str) {
            Ok(parsed_query) => parsed_query,
            Err(_) => {
                WARNING!(target: LOGGER_TARGET, function: function_name, "Can't parse query: {}", query_str);
                return Arc::new(RoaringBitmap::new());
            }
        };
        let searched_bitmap = match searcher.search(
            &text_query,
            &RowIdRoaringCollector::with_field("row_id".to_string()),
        ) {
            Ok(result_bitmap) => result_bitmap,
            Err(e) => {
                WARNING!(target: LOGGER_TARGET, function: function_name, "Can't execute search in text_query: {}", e);
                return Arc::new(RoaringBitmap::new());
            }
        };
        searched_bitmap
    }

    
}

fn intersect_and_return(row_id_roaring_bitmap: Arc<RoaringBitmap>, lrange: u64, rrange: u64) -> Result<Arc<RoaringBitmap>, SearchError> {
    let mut row_id_range = RoaringBitmap::new();
    row_id_range.insert_range(lrange as u32..(rrange + 1) as u32);
    row_id_range &= Arc::as_ref(&row_id_roaring_bitmap);
    Ok(Arc::new(row_id_range))
}
/// Performs a search operation using the given index reader, query, and range.
///
/// Arguments:
/// - `ir`: A mutable pointer to `IndexR` containing the index and reader.
/// - `query_ptr`: A pointer to the query string.
/// - `lrange`: The lower bound of the row ID range.
/// - `rrange`: The upper bound of the row ID range.
/// - `function_name`: The name of the calling function for logging purposes.
///
/// Returns:
/// - `Result<RoaringBitmap, SearchError>`: A `RoaringBitmap` containing the search results,
///   or an error if the search fails.
fn perform_search(
    ir: *mut IndexR,
    query_ptr: *const c_char,
    lrange: u64,
    rrange: u64,
    function_name: &str,
    use_regrex: bool
) -> Result<Arc<RoaringBitmap>, SearchError> {
    // Check for null pointers.
    let indexr = unsafe {
        if ir.is_null() {
            ERROR!(target: LOGGER_TARGET, function: function_name, "Index reader can't be null.");
            return Err(SearchError::NullIndexReader);
        }
        &*ir
    };

    let query_str = unsafe {
        if query_ptr.is_null() {
            ERROR!(target: LOGGER_TARGET, function: function_name, "Query pointer can't be null.");
            return Err(SearchError::InvalidQueryStr);
        }
        CStr::from_ptr(query_ptr)
            .to_str()
            .map_err(|_| SearchError::InvalidQueryStr)?
    };


    #[cfg(feature = "use-flurry-cache")]
    {
        // Resolve cache or compute the roaring bitmap for the given query.
        let row_id_roaring_bitmap = CACHE_FOR_SKIP_INDEX.resolve(
            (ir as usize, query_str.to_string(), indexr.path.to_string()),
            || compute_bitmap(&indexr, query_str, function_name, use_regrex),
        );
        intersect_and_return(row_id_roaring_bitmap, lrange, rrange)
    }
    #[cfg(not(feature = "use-flurry-cache"))]
    {
        // not use cache
        let row_id_roaring_bitmap = compute_bitmap(&indexr, query_str, function_name, use_regrex);
        intersect_and_return(row_id_roaring_bitmap, lrange, rrange)
    }

}

/// Determines if a query string appears within a specified row ID range.
///
/// Arguments:
/// - `ir`: Pointer to the index reader.
/// - `query_ptr`: Pointer to the query string.
/// - `lrange`: The left (inclusive) boundary of the row ID range.
/// - `rrange`: The right (inclusive) boundary of the row ID range.
///
/// Returns:
/// - `true` if the query string appears in the given row ID range, `false` otherwise.
#[no_mangle]
pub extern "C" fn tantivy_search_in_rowid_range(
    ir: *mut IndexR,
    query_ptr: *const c_char,
    lrange: u64,
    rrange: u64,
    use_regrex: bool,
) -> bool {
    match perform_search(
        ir,
        query_ptr,
        lrange,
        rrange,
        TANTIVY_SEARCH_IN_ROWID_RANGE_NAME,
        use_regrex,
    ) {
        Ok(row_id_range) => !row_id_range.is_empty(),
        Err(e) => {
            ERROR!(target:LOGGER_TARGET, function: TANTIVY_SEARCH_IN_ROWID_RANGE_NAME, "Error in search: {}", e);
            false
        }
    }
}

/// Counts the occurrences of a query string within a specified row ID range.
///
/// Arguments:
/// - `ir`: Pointer to the index reader.
/// - `query_ptr`: Pointer to the query string.
/// - `lrange`: The left (inclusive) boundary of the row ID range.
/// - `rrange`: The right (inclusive) boundary of the row ID range.
///
/// Returns:
/// - The count of occurrences of the query string within the row ID range.
#[no_mangle]
pub extern "C" fn tantivy_count_in_rowid_range(
    ir: *mut IndexR,
    query_ptr: *const c_char,
    lrange: u64,
    rrange: u64,
    use_regex: bool,
) -> c_uint {
    match perform_search(
        ir,
        query_ptr,
        lrange,
        rrange,
        TANTIVY_COUNT_IN_ROWID_RANGE_NAME,
        use_regex,
    ) {
        Ok(row_id_range) => row_id_range.len() as u32,
        Err(e) => {
            ERROR!(target: LOGGER_TARGET, function: TANTIVY_COUNT_IN_ROWID_RANGE_NAME, "Error in search: {}", e);
            0
        }
    }
}

/// Indexes a document.
///
/// Arguments:
/// - `iw`: Pointer to the index writer.
/// - `row_id_`: Row ID associated with the document.
/// - `text_`: Pointer to the text data of the document.
///
/// Returns:
/// - A non-zero value if successful, zero otherwise.
#[no_mangle]
pub extern "C" fn tantivy_index_doc(iw: *mut IndexW, row_id_: u64, text_: *const c_char) -> bool {
    if iw.is_null() || text_.is_null() {
        ERROR!(target: LOGGER_TARGET, function: TANTIVY_INDEX_DOC_NAME, "Invalid arguments to tantivy_index_doc");
        return false;
    }

    let text_str = unsafe {
        match CStr::from_ptr(text_).to_str() {
            Ok(str) => str,
            Err(_) => {
                ERROR!(target: LOGGER_TARGET, function: TANTIVY_INDEX_DOC_NAME, "Failed to convert text to string");
                return false;
            }
        }
    };

    // 'iw' is a mutable reference. Exiting this scope will destroy 'iw', but the original object pointed to by '*iw' will not be dropped.
    let iw = unsafe { &mut *iw };
    let schema = iw.index.schema();
    let text_field = match schema.get_field("text") {
        Ok(text_field_) => text_field_,
        Err(_) => {
            ERROR!(target: LOGGER_TARGET, function: TANTIVY_INDEX_DOC_NAME, "Failed to get text field");
            return false;
        }
    };
    let row_id_field = match schema.get_field("row_id") {
        Ok(row_id_field_) => row_id_field_,
        Err(_) => {
            ERROR!(target: LOGGER_TARGET, function: TANTIVY_INDEX_DOC_NAME, "Failed to get row_id field");
            return false;
        }
    };
    let mut doc = Document::default();
    doc.add_u64(row_id_field, row_id_);
    doc.add_text(text_field, text_str);

    if iw.writer.add_document(doc).is_ok() {
        true
    } else {
        ERROR!(target: LOGGER_TARGET, function: TANTIVY_INDEX_DOC_NAME, "Failed to add document");
        false
    }
}

/// Commits the changes to the index, writing it to the file system.
///
/// Arguments:
/// - `iw`: Pointer to the index writer.
///
/// Returns:
/// - A non-zero value if successful, zero otherwise.
#[no_mangle]
pub extern "C" fn tantivy_writer_commit(iw: *mut IndexW) -> bool {
    if iw.is_null() {
        ERROR!(target:LOGGER_TARGET, function: TANTIVY_WRITER_COMMIT_NAME, "Invalid index writer pointer");
        return false;
    }
    // 'iw' is a mutable reference. Exiting this scope will destroy 'iw', but the original object pointed to by '*iw' will not be dropped.
    let iw = unsafe { &mut *iw };
    match iw.writer.commit() {
        Ok(_) => true,
        Err(e) => {
            ERROR!(target:LOGGER_TARGET, function: TANTIVY_WRITER_COMMIT_NAME, "Failed to commit writer: {}", e);
            false
        }
    }
}

/// Frees the index reader.
///
/// Arguments:
/// - `ir`: Pointer to the index reader.
#[no_mangle]
pub extern "C" fn tantivy_reader_free(ir: *mut IndexR) {
    if ir.is_null() {
        ERROR!(target:LOGGER_TARGET, function: TANTIVY_READER_FREE_NAME, "Invalid index reader pointer");
        return;
    }
    drop(unsafe { Box::from_raw(ir) });
}

/// Frees the index writer and waits for any merging threads to complete.
///
/// Arguments:
/// - `iw`: Pointer to the index writer.
#[no_mangle]
pub extern "C" fn tantivy_writer_free(iw: *mut IndexW) {
    if iw.is_null() {
        ERROR!(target:LOGGER_TARGET, function: TANTIVY_WRITER_FREE_NAME, "Invalid index writer pointer");
        return;
    }

    let iw = unsafe { Box::from_raw(iw) };
    if let Err(e) = iw.writer.wait_merging_threads() {
        ERROR!(target:LOGGER_TARGET, function: TANTIVY_WRITER_FREE_NAME, "Error while waiting for merging threads: {:?}", e);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
