use ffi::RowIdWithScore;
use libc::*;
use std::{cmp::Ordering, ffi::CStr};

pub mod common;
mod commons;
mod flurry_cache;
pub mod index;
mod logger;
pub mod search;
mod tokenizer;

use commons::*;
use index::index_manager::*;
use logger::ffi_logger::*;
use search::index_searcher::*;

#[cxx::bridge]
pub mod ffi {

    #[derive(Debug, Clone)]
    pub struct RowIdWithScore {
        pub row_id: u64,
        pub score: f32,
        pub seg_id: u32,
        pub doc_id: u32,
        pub doc: String,
    }

    extern "Rust" {
        // fn tantivy_logger_initialize(log_path: &CxxString, log_level: &CxxString, console_logging: bool, callback: LogCallback, enable_callback: bool, only_tantivy_search: bool) -> Result<bool>;
        fn tantivy_create_index_with_tokenizer(
            index_path: &CxxString,
            tokenizer_with_parameter: &CxxString,
        ) -> Result<bool>;
        fn tantivy_create_index(index_path: &CxxString) -> Result<bool>;
        fn tantivy_load_index(index_path: &CxxString) -> Result<bool>;
        fn tantivy_index_doc(index_path: &CxxString, row_id: u64, doc: &CxxString) -> Result<bool>;
        fn tantivy_writer_commit(index_path: &CxxString) -> Result<bool>;
        fn tantivy_reader_free(index_path: &CxxString) -> Result<bool>;
        fn tantivy_writer_free(index_path: &CxxString) -> Result<bool>;
        fn tantivy_search_in_rowid_range(
            index_path: &CxxString,
            query: &CxxString,
            lrange: u64,
            rrange: u64,
            use_regex: bool,
        ) -> Result<bool>;
        fn tantivy_count_in_rowid_range(
            index_path: &CxxString,
            query: &CxxString,
            lrange: u64,
            rrange: u64,
            use_regex: bool,
        ) -> Result<u64>;
        fn tantivy_search_bm25_with_filter(
            index_path: &CxxString,
            query: &CxxString,
            row_ids: &CxxVector<u32>,
            top_k: u32,
            need_text: bool,
        ) -> Result<Vec<RowIdWithScore>>;
        fn tantivy_search_bm25(
            index_path: &CxxString,
            query: &CxxString,
            top_k: u32,
            need_text: bool,
        ) -> Result<Vec<RowIdWithScore>>;
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

impl PartialOrd for RowIdWithScore {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowIdWithScore {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reversed score ordering to make BinaryHeap work as a min-heap
        let by_score = other
            .score
            .partial_cmp(&self.score)
            .unwrap_or(Ordering::Equal);
        // In case of a tie on the score, we sort by ascending
        // row_id, seg_id, and doc_id in order to ensure a stable sorting, work as a max-heap.
        let lazy_by_row_id = || self.row_id.cmp(&other.row_id);
        let lazy_by_seg_id = || self.seg_id.cmp(&other.seg_id);
        let lazy_by_doc_id = || self.doc_id.cmp(&other.doc_id);
        let lazy_by_doc = || self.doc.cmp(&other.doc);

        by_score
            .then_with(lazy_by_row_id)
            .then_with(lazy_by_seg_id)
            .then_with(lazy_by_doc_id)
            .then_with(lazy_by_doc)
    }
}

impl PartialEq for RowIdWithScore {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for RowIdWithScore {}

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, collections::BinaryHeap};

    use crate::ffi::RowIdWithScore;

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn test_row_id_with_score() {
        // compare score: reverse binary_heap
        let riws0 = RowIdWithScore {
            row_id: 0,
            score: 1.11,
            seg_id: 0,
            doc_id: 0,
            doc: "abc".to_string(),
        };
        let riws1 = RowIdWithScore {
            row_id: 0,
            score: 1.11,
            seg_id: 0,
            doc_id: 0,
            doc: "abc".to_string(),
        };
        let riws2 = RowIdWithScore {
            row_id: 0,
            score: 2.22,
            seg_id: 0,
            doc_id: 0,
            doc: "abc".to_string(),
        };
        // test for min_binary_heap
        let mut heap: BinaryHeap<RowIdWithScore> = BinaryHeap::new();
        heap.push(riws0.clone());
        heap.push(riws1.clone());
        heap.push(riws2.clone());
        assert_eq!(heap.peek().unwrap(), &riws1);
        assert_eq!(heap.peek().unwrap(), &riws0);
        assert_eq!(riws1.cmp(&riws2), Ordering::Greater);

        // compare with `row_id`
        let riws3 = RowIdWithScore {
            row_id: 0,
            score: 3.33,
            seg_id: 1,
            doc_id: 1,
            doc: "def".to_string(),
        };
        let riws4 = RowIdWithScore {
            row_id: 1,
            score: 3.33,
            seg_id: 0,
            doc_id: 0,
            doc: "abc".to_string(),
        };
        heap.push(riws3.clone());
        heap.push(riws4.clone());
        assert_eq!(heap.peek().unwrap(), &riws1);
        assert_eq!(riws4.cmp(&riws3), Ordering::Greater);

        // compare with `seg_id`
        let riws5 = RowIdWithScore {
            row_id: 2,
            score: 4.44,
            seg_id: 0,
            doc_id: 2,
            doc: "def".to_string(),
        };
        let riws6 = RowIdWithScore {
            row_id: 2,
            score: 4.44,
            seg_id: 1,
            doc_id: 1,
            doc: "abc".to_string(),
        };
        heap.push(riws3.clone());
        heap.push(riws4.clone());
        assert_eq!(heap.peek().unwrap(), &riws1);
        assert_eq!(riws6.cmp(&riws5), Ordering::Greater);

        // compare with `doc_id`
        let riws7 = RowIdWithScore {
            row_id: 3,
            score: 5.55,
            seg_id: 1,
            doc_id: 1,
            doc: "def".to_string(),
        };
        let riws8 = RowIdWithScore {
            row_id: 3,
            score: 5.55,
            seg_id: 1,
            doc_id: 2,
            doc: "abc".to_string(),
        };
        heap.push(riws3.clone());
        heap.push(riws4.clone());
        assert_eq!(heap.peek().unwrap(), &riws1);
        assert_eq!(riws8.cmp(&riws7), Ordering::Greater);

        // compare with `doc`
        let riws9 = RowIdWithScore {
            row_id: 4,
            score: 6.66,
            seg_id: 2,
            doc_id: 2,
            doc: "abc".to_string(),
        };
        let riws10 = RowIdWithScore {
            row_id: 4,
            score: 6.66,
            seg_id: 2,
            doc_id: 2,
            doc: "acd".to_string(),
        };
        heap.push(riws3.clone());
        heap.push(riws4.clone());
        assert_eq!(heap.peek().unwrap(), &riws1);
        assert_eq!(riws10.cmp(&riws9), Ordering::Greater);

        // compare `equal`
        let riws11 = RowIdWithScore {
            row_id: 4,
            score: 1.11,
            seg_id: 2,
            doc_id: 2,
            doc: "abc".to_string(),
        };
        let riws12 = RowIdWithScore {
            row_id: 4,
            score: 1.11,
            seg_id: 2,
            doc_id: 2,
            doc: "abc".to_string(),
        };
        heap.push(riws11.clone());
        heap.push(riws12.clone());
        assert_eq!(heap.peek().unwrap(), &riws11);
        assert_eq!(riws12.cmp(&riws11), Ordering::Equal);
    }
}
