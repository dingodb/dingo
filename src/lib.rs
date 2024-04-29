use ffi::BM25Result;
use ffi::BoolResult;
use ffi::DocWithFreq;
use ffi::FieldTokenNums;
use ffi::RowIdWithScore;
use ffi::Statistics;
use std::cmp::Ordering;

mod common;
mod index;
mod logger;
mod search;
mod tokenizer;
mod utils;
use common::constants::*;
use index::api::api_index::*;
use search::api::api_clickhouse::*;
use search::api::api_common::*;
use search::api::api_dingo::*;
use utils::ffi_utils::*;
// re-export log ffi function.
pub use logger::ffi_logger::*;

#[cxx::bridge]
pub mod ffi {

    #[derive(Debug, Clone)]
    pub struct RowIdWithScore {
        pub row_id: u64,
        pub score: f32,
        pub seg_id: u32,
        pub doc_id: u32,
        pub docs: Vec<String>,
    }

    #[derive(Debug, Clone)]
    pub struct BM25Result {
        result: Vec<RowIdWithScore>,
        error_code: i32,
        error_msg: String,
    }

    #[derive(Debug, Clone)]
    pub struct BoolResult {
        result: bool,
        error_code: i32,
        error_msg: String,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct DocWithFreq {
        pub term_str: String,
        pub field_id: u32,
        pub doc_freq: u64,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct FieldTokenNums {
        pub field_id: u32,
        pub field_total_tokens: u64,
    }

    #[derive(Debug, Clone)]
    pub struct Statistics {
        pub docs_freq: Vec<DocWithFreq>,
        // pub total_num_tokens: Vec<FieldTokenNums>,
        pub total_num_tokens: u64,
        pub total_num_docs: u64,
    }

    extern "Rust" {
        pub fn ffi_varify_index_parameter(index_json_parameter: &CxxString) -> BoolResult;

        /// Create tantivy index.
        /// arguments:
        /// - `index_path`: index directory.
        /// - `column_names`: which columns will be used to build index.
        /// - `index_json_parameter`: config index with json.
        fn ffi_create_index_with_parameter(
            index_path: &CxxString,
            column_names: &CxxVector<CxxString>,
            index_json_parameter: &CxxString,
        ) -> BoolResult;

        /// Create tantivy index by default.
        /// argements:
        /// - `index_path`: index directory.
        /// - `column_names`: which columns will be used to build index.
        fn ffi_create_index(
            index_path: &CxxString,
            column_names: &CxxVector<CxxString>,
        ) -> BoolResult;

        /// Index multi column docs with given rowId.
        /// arguments:
        /// - `index_path`: index directory.
        /// - `row_id`: row_id given by ClickHouse.
        /// - `column_names`: align with column_docs.
        /// - `column_docs`: align with column_names.
        fn ffi_index_multi_column_docs(
            index_path: &CxxString,
            row_id: u64,
            column_names: &CxxVector<CxxString>,
            column_docs: &CxxVector<CxxString>,
        ) -> BoolResult;

        /// Index multi column docs with given rowId.
        /// arguments:
        /// - `index_path`: index directory.
        /// - `row_id`: row_id given by ClickHouse.
        /// - `text_column_names`: align with column_docs.
        /// - `text_column_docs`: align with column_names.
        /// - `i64_column_names`: align with column_docs.
        /// - `i64_column_docs`: align with column_names.
        /// - `f64_column_names`: align with column_docs.
        /// - `f64_column_docs`: align with column_names.
        fn ffi_index_multi_type_column_docs(
            index_path: &CxxString,
            row_id: u64,
            text_column_names: &CxxVector<CxxString>,
            text_column_docs: &CxxVector<CxxString>,
            i64_column_names: &CxxVector<CxxString>,
            i64_column_docs: &CxxVector<i64>,
            f64_column_names: &CxxVector<CxxString>,
            f64_column_docs: &CxxVector<f64>,
        ) -> BoolResult;

        /// Delete a group of rowIds.
        /// arguments:
        /// - `index_path`: index directory.
        /// - `row_ids`: a group of rowIds need be deleted.
        fn ffi_delete_row_ids(index_path: &CxxString, row_ids: &CxxVector<u32>) -> BoolResult;

        /// Commit index writer
        /// arguments:
        /// - `index_path`: index directory.
        fn ffi_index_writer_commit(index_path: &CxxString) -> BoolResult;

        /// Free index writer
        /// arguments:
        /// - `index_path`: index directory.
        fn ffi_free_index_writer(index_path: &CxxString) -> BoolResult;

        /// Load index writer
        /// arguments:
        /// - `index_path`: index directory.
        fn ffi_load_index_writer(index_path: &CxxString) -> BoolResult;

        /// Do index reader reload
        /// arguments:
        /// - `index_path`: index directory.
        pub fn ffi_index_reader_reload(index_path: &CxxString) -> BoolResult;

        /// Load index reader
        /// arguments:
        /// - `index_path`: index directory.
        fn ffi_load_index_reader(index_path: &CxxString) -> BoolResult;

        /// Free index reader
        /// arguments:
        /// - `index_path`: index directory.
        fn ffi_free_index_reader(index_path: &CxxString) -> BoolResult;

        /// Get indexed docs numbers.
        /// arguments:
        /// - `index_path`: index directory.
        fn ffi_get_indexed_doc_counts(index_path: &CxxString) -> u64;

        /// Execute single term query with given rowId range.
        /// arguments:
        /// - `index_path`: index directory.
        /// - `column_name`: which column will execute search.
        /// - `term`: term needs to be searched.
        /// - `lrange`: rowId left begin.
        /// - `rrange`: rowId right end.
        fn ffi_query_term_with_range(
            index_path: &CxxString,
            column_name: &CxxString,
            term: &CxxString,
            lrange: u64,
            rrange: u64,
        ) -> BoolResult;

        /// Execute a group of terms query with given rowId range.
        /// arguments:
        /// - `index_path`: index directory.
        /// - `column_name`: which column will execute search.
        /// - `terms`: a group of terms need to be searched.
        /// - `lrange`: rowId left begin.
        /// - `rrange`: rowId right end.
        fn ffi_query_terms_with_range(
            index_path: &CxxString,
            column_name: &CxxString,
            terms: &CxxVector<CxxString>,
            lrange: u64,
            rrange: u64,
        ) -> BoolResult;

        /// Execute a sentence query with given rowId range.
        /// arguments:
        /// - `index_path`: index directory.
        /// - `column_name`: which column will execute search.
        /// - `sentence`: sentence needs to be searched.
        /// - `lrange`: rowId left begin.
        /// - `rrange`: rowId right end.
        fn ffi_query_sentence_with_range(
            index_path: &CxxString,
            column_name: &CxxString,
            sentence: &CxxString,
            lrange: u64,
            rrange: u64,
        ) -> BoolResult;

        /// Execute a regex query with given rowId range.
        /// arguments:
        /// - `index_path`: index directory.
        /// - `column_name`: which column will execute search.
        /// - `pattern`: pattern should be given by ClickHouse.
        /// - `lrange`: rowId left begin.
        /// - `rrange`: rowId right end.
        fn ffi_regex_term_with_range(
            index_path: &CxxString,
            column_name: &CxxString,
            pattern: &CxxString,
            lrange: u64,
            rrange: u64,
        ) -> BoolResult;

        /// Execute a term query and return rowIds u8 bitmap.
        /// arguments:
        /// - `index_path`: index directory.
        /// - `column_name`: which column will execute search.
        /// - `term`: term needs to be searched.
        pub fn ffi_query_term_bitmap(
            index_path: &CxxString,
            column_name: &CxxString,
            term: &CxxString,
        ) -> Vec<u8>;

        /// Execute a group of terms query and return rowIds u8 bitmap.
        /// arguments:
        /// - `index_path`: index directory.
        /// - `column_name`: which column will execute search.
        /// - `terms`: terms need to be searched.
        pub fn ffi_query_terms_bitmap(
            index_path: &CxxString,
            column_name: &CxxString,
            terms: &CxxVector<CxxString>,
        ) -> Vec<u8>;

        /// Execute a sentence query and return rowIds u8 bitmap.
        /// arguments:
        /// - `index_path`: index directory.
        /// - `column_name`: which column will execute search.
        /// - `sentence`: sentence needs to be searched.
        pub fn ffi_query_sentence_bitmap(
            index_path: &CxxString,
            column_name: &CxxString,
            sentence: &CxxString,
        ) -> Vec<u8>;

        /// Execute a regex query and return rowIds u8 bitmap.
        /// arguments:
        /// - `index_path`: index directory.
        /// - `column_name`: which column will execute search.
        /// - `pattern`: pattern should be given by ClickHouse.
        pub fn ffi_regex_term_bitmap(
            index_path: &CxxString,
            column_name: &CxxString,
            pattern: &CxxString,
        ) -> Vec<u8>;

        /// Execute a regex query and return rowIds u8 bitmap.
        /// arguments:
        /// - `index_path`: index directory.
        /// - `sentence`: from ClickHouse TextSearch function.
        /// - `topk`: only return top k related results.
        /// - `u8_aived_bitmap`: alived rowIds given by u8 bitmap.
        /// - `query_with_filter`: whether use alived_bitmap or not.
        pub fn ffi_bm25_search(
            index_path: &CxxString,
            sentence: &CxxString,
            topk: u32,
            u8_aived_bitmap: &CxxVector<u8>,
            query_with_filter: bool,
        ) -> BM25Result;

        /// Execute a regex query and return rowIds u8 bitmap.
        /// arguments:
        /// - `index_path`: index directory.
        /// - `sentence`: from ClickHouse TextSearch function.
        /// - `topk`: only return top k related results.
        /// - `u8_aived_bitmap`: alived rowIds given by u8 bitmap.
        /// - `query_with_filter`: whether use alived_bitmap or not.
        /// - `colunm_names`: for multi column search.
        pub fn ffi_bm25_search_with_column_names(
            index_path: &CxxString,
            sentence: &CxxString,
            topk: u32,
            u8_aived_bitmap: &CxxVector<u8>,
            query_with_filter: bool,
            column_names: &CxxVector<CxxString>,
        ) -> BM25Result;

        /// Get doc freq for current part.
        /// arguments:
        /// - `index_path`: index directory.
        /// - `sentence`: query_str.
        pub fn ffi_get_doc_freq(index_path: &CxxString, sentence: &CxxString) -> Vec<DocWithFreq>;

        /// Get total num docs for current part.
        /// arguments:
        /// - `index_path`: index directory.
        pub fn ffi_get_total_num_docs(index_path: &CxxString) -> u64;

        /// Get total num tokens for current part.
        /// arguments:
        /// - `index_path`: index directory.
        pub fn ffi_get_total_num_tokens(index_path: &CxxString) -> u64;
    }
}

// pub type LogCallback = extern "C" fn(i32, *const c_char, *const c_char);

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

        by_score
            .then_with(lazy_by_row_id)
            .then_with(lazy_by_seg_id)
            .then_with(lazy_by_doc_id)
    }
}

impl PartialEq for RowIdWithScore {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for RowIdWithScore {}

#[allow(dead_code)]
impl DocWithFreq {
    fn new(term_str: String, field_id: u32, doc_freq: u64) -> Self {
        DocWithFreq {
            term_str,
            field_id,
            doc_freq,
        }
    }
}

#[allow(dead_code)]
impl Statistics {
    fn new(docs_freq: Vec<DocWithFreq>, total_num_tokens: u64, total_num_docs: u64) -> Self {
        Statistics {
            docs_freq,
            total_num_tokens,
            total_num_docs,
        }
    }
}
#[allow(dead_code)]
impl FieldTokenNums {
    fn new(field_id: u32, field_total_tokens: u64) -> Self {
        FieldTokenNums {
            field_id,
            field_total_tokens,
        }
    }
}

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
            docs: vec![],
        };
        let riws1 = RowIdWithScore {
            row_id: 0,
            score: 1.11,
            seg_id: 0,
            doc_id: 0,
            docs: vec![],
        };
        let riws2 = RowIdWithScore {
            row_id: 0,
            score: 2.22,
            seg_id: 0,
            doc_id: 0,
            docs: vec![],
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
            docs: vec![],
        };
        let riws4 = RowIdWithScore {
            row_id: 1,
            score: 3.33,
            seg_id: 0,
            doc_id: 0,
            docs: vec![],
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
            docs: vec![],
        };
        let riws6 = RowIdWithScore {
            row_id: 2,
            score: 4.44,
            seg_id: 1,
            doc_id: 1,
            docs: vec![],
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
            docs: vec![],
        };
        let riws8 = RowIdWithScore {
            row_id: 3,
            score: 5.55,
            seg_id: 1,
            doc_id: 2,
            docs: vec![],
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
            docs: vec![],
        };
        let riws10 = RowIdWithScore {
            row_id: 4,
            score: 6.66,
            seg_id: 2,
            doc_id: 2,
            docs: vec![],
        };
        heap.push(riws3.clone());
        heap.push(riws4.clone());
        assert_eq!(heap.peek().unwrap(), &riws1);
        assert_eq!(riws10.cmp(&riws9), Ordering::Equal);

        // compare `equal`
        let riws11 = RowIdWithScore {
            row_id: 4,
            score: 1.11,
            seg_id: 2,
            doc_id: 2,
            docs: vec![],
        };
        let riws12 = RowIdWithScore {
            row_id: 4,
            score: 1.11,
            seg_id: 2,
            doc_id: 2,
            docs: vec![],
        };
        heap.push(riws11.clone());
        heap.push(riws12.clone());
        assert_eq!(heap.peek().unwrap(), &riws11);
        assert_eq!(riws12.cmp(&riws11), Ordering::Equal);
    }
}
