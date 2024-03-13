use ffi::RowIdWithScore;
use std::cmp::Ordering;

mod common;
mod index;
mod logger;
mod search;
mod tokenizer;
mod utils;
use common::constants::*;
use index::ffi_index_manager::*;
use search::ffi_index_searcher::*;
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

    extern "Rust" {
        /// Creates an index using a specified tokenizer (e.g., Chinese, English, Japanese, etc.).
        ///
        /// Arguments:
        /// - `index_path`: The directory path for building the index.
        /// - `tokenizer_with_parameter`: A str contains tokenizer name and parameters.
        ///
        /// Returns:
        /// - A bool value represent operation success.
        fn ffi_create_index_with_parameter(
            index_path: &CxxString,
            column_names: &CxxVector<CxxString>,
            index_json_parameter: &CxxString,
        ) -> Result<bool>;

        /// Creates an index using the default tokenizer.
        ///
        /// Arguments:
        /// - `index_path`: The directory path for building the index.
        ///
        /// Returns:
        /// - A bool value represent operation success.
        fn ffi_create_index(
            index_path: &CxxString,
            column_names: &CxxVector<CxxString>,
        ) -> Result<bool>;

        /// Indexes a document.
        ///
        /// Arguments:
        /// - `index_path`: The directory path for building the index.
        /// - `row_id`: Row ID associated with the document.
        /// - `doc`: The text data of the document.
        ///
        /// Returns:
        /// - A bool value represent operation success.        
        fn ffi_index_multi_column_docs(
            index_path: &CxxString,
            row_id: u64,
            column_names: &CxxVector<CxxString>,
            column_docs: &CxxVector<CxxString>,
        ) -> Result<bool>;

        /// Delete a group of row_ids.
        ///
        /// Arguments:
        /// - `index_path`: The directory path for building the index.
        /// - `row_ids`: a group of row_ids that needs to be deleted.
        ///
        /// Returns:
        /// - A bool value represent operation success.
        fn ffi_delete_row_ids(index_path: &CxxString, row_ids: &CxxVector<u32>) -> Result<bool>;

        /// Commits the changes to the index, writing it to the file system.
        ///
        /// Arguments:
        /// - `index_path`: The directory path for building the index.
        ///
        /// Returns:
        /// - A bool value represent operation success.
        fn ffi_index_writer_commit(index_path: &CxxString) -> Result<bool>;

        /// Frees the index writer and waits for all merging threads to complete.
        ///
        /// Arguments:
        /// - `index_path`: The directory path for building the index.
        ///
        /// Returns:
        /// - A bool value represent operation success.
        fn ffi_index_writer_free(index_path: &CxxString) -> Result<bool>;

        /// Loads an index from a specified directory.
        ///
        /// Arguments:
        /// - `index_path`: The directory path for building the index.
        ///
        /// Returns:
        /// - A bool value represent operation success.
        fn ffi_load_index(index_path: &CxxString) -> Result<bool>;

        /// Frees the index reader.
        ///
        /// Arguments:
        /// - `index_path`: The directory path for building the index.
        ///
        /// Returns:
        /// - A bool value represent operation success.
        fn ffi_free_reader(index_path: &CxxString) -> Result<bool>;

        /// Determines if a query string appears within a specified row ID range.
        ///
        /// Arguments:
        /// - `index_path`: The directory path for building the index.
        /// - `query`: Query string.
        /// - `lrange`: The left (inclusive) boundary of the row ID range.
        /// - `rrange`: The right (inclusive) boundary of the row ID range.
        /// - `use_regex`: Whether use regex searcher.
        ///
        /// Returns:
        /// - A bool value represent whether granule hitted.
        fn ffi_search_in_rowid_range(
            index_path: &CxxString,
            column_name: &CxxString,
            query: &CxxString,
            lrange: u64,
            rrange: u64,
            use_regex: bool,
        ) -> Result<bool>;

        /// Counts the occurrences of a query string within a specified row ID range.
        ///
        /// Arguments:
        /// - `index_path`: The directory path for building the index.
        /// - `query`: Query string.
        /// - `lrange`: The left (inclusive) boundary of the row ID range.
        /// - `rrange`: The right (inclusive) boundary of the row ID range.
        /// - `use_regex`: Whether use regex searcher.
        ///
        /// Returns:
        /// - The count of occurrences of the query string within the row ID range.
        fn ffi_count_in_rowid_range(
            index_path: &CxxString,
            column_name: &CxxString,
            query: &CxxString,
            lrange: u64,
            rrange: u64,
            use_regex: bool,
        ) -> Result<u64>;

        /// Execute bm25_search with filter row_ids.
        ///
        /// Arguments:
        /// - `index_path`: The directory path for building the index.
        /// - `query`: Query string.
        /// - `u8_bitmap`: A vector<u8> bitmap represent row_ids need to be filtered.
        /// - `top_k`: Try to search `k` results.
        /// - `need_text`: Whether need return origin doc content.
        ///
        /// Returns:
        /// - A group of RowIdWithScore Objects.
        fn ffi_bm25_search_with_filter(
            index_path: &CxxString,
            column_names: &CxxVector<CxxString>,
            query: &CxxString,
            u8_bitmap: &CxxVector<u8>,
            top_k: u32,
            need_text: bool,
        ) -> Result<Vec<RowIdWithScore>>;

        /// Execute bm25_search.
        ///
        /// Arguments:
        /// - `index_path`: The directory path for building the index.
        /// - `query`: Query string.
        /// - `top_k`: Try to search `k` results.
        /// - `need_text`: Whether need return origin doc content.
        ///
        /// Returns:
        /// - A group of RowIdWithScore Objects.
        fn ffi_bm25_search(
            index_path: &CxxString,
            column_names: &CxxVector<CxxString>,
            query: &CxxString,
            top_k: u32,
            need_text: bool,
        ) -> Result<Vec<RowIdWithScore>>;

        /// Execute search with like pattern or not.
        ///
        /// Arguments:
        /// - `index_path`: The directory path for building the index.
        /// - `query`: Query should be like pattern.
        /// - `use_regex`: For like pattern, use_regex should be true.
        ///
        /// Returns:
        /// - row_ids u8 bitmap.
        pub fn ffi_search_bitmap_results(
            index_path: &CxxString,
            column_names: &CxxVector<CxxString>,
            query: &CxxString,
            use_regex: bool,
        ) -> Result<Vec<u8>>;

        /// Get the number of documents stored in the index file.
        /// In general, we can consider the number of stored documents as 'n',
        /// and the range of row_id is [0, n-1].
        /// Arguments:
        /// - `index_path`: The directory path for building the index.
        ///
        /// Returns:
        /// - The count of documents stored in the index file.
        fn ffi_indexed_doc_counts(index_path: &CxxString) -> Result<u64>;

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

// #[cfg(test)]
// mod tests {
//     use std::{cmp::Ordering, collections::BinaryHeap};

//     use crate::ffi::RowIdWithScore;

//     #[test]
//     fn it_works() {
//         assert_eq!(2 + 2, 4);
//     }

//     #[test]
//     fn test_row_id_with_score() {
//         // compare score: reverse binary_heap
//         let riws0 = RowIdWithScore {
//             row_id: 0,
//             score: 1.11,
//             seg_id: 0,
//             doc_id: 0,
//             doc: "abc".to_string(),
//         };
//         let riws1 = RowIdWithScore {
//             row_id: 0,
//             score: 1.11,
//             seg_id: 0,
//             doc_id: 0,
//             doc: "abc".to_string(),
//         };
//         let riws2 = RowIdWithScore {
//             row_id: 0,
//             score: 2.22,
//             seg_id: 0,
//             doc_id: 0,
//             doc: "abc".to_string(),
//         };
//         // test for min_binary_heap
//         let mut heap: BinaryHeap<RowIdWithScore> = BinaryHeap::new();
//         heap.push(riws0.clone());
//         heap.push(riws1.clone());
//         heap.push(riws2.clone());
//         assert_eq!(heap.peek().unwrap(), &riws1);
//         assert_eq!(heap.peek().unwrap(), &riws0);
//         assert_eq!(riws1.cmp(&riws2), Ordering::Greater);

//         // compare with `row_id`
//         let riws3 = RowIdWithScore {
//             row_id: 0,
//             score: 3.33,
//             seg_id: 1,
//             doc_id: 1,
//             doc: "def".to_string(),
//         };
//         let riws4 = RowIdWithScore {
//             row_id: 1,
//             score: 3.33,
//             seg_id: 0,
//             doc_id: 0,
//             doc: "abc".to_string(),
//         };
//         heap.push(riws3.clone());
//         heap.push(riws4.clone());
//         assert_eq!(heap.peek().unwrap(), &riws1);
//         assert_eq!(riws4.cmp(&riws3), Ordering::Greater);

//         // compare with `seg_id`
//         let riws5 = RowIdWithScore {
//             row_id: 2,
//             score: 4.44,
//             seg_id: 0,
//             doc_id: 2,
//             doc: "def".to_string(),
//         };
//         let riws6 = RowIdWithScore {
//             row_id: 2,
//             score: 4.44,
//             seg_id: 1,
//             doc_id: 1,
//             doc: "abc".to_string(),
//         };
//         heap.push(riws3.clone());
//         heap.push(riws4.clone());
//         assert_eq!(heap.peek().unwrap(), &riws1);
//         assert_eq!(riws6.cmp(&riws5), Ordering::Greater);

//         // compare with `doc_id`
//         let riws7 = RowIdWithScore {
//             row_id: 3,
//             score: 5.55,
//             seg_id: 1,
//             doc_id: 1,
//             doc: "def".to_string(),
//         };
//         let riws8 = RowIdWithScore {
//             row_id: 3,
//             score: 5.55,
//             seg_id: 1,
//             doc_id: 2,
//             doc: "abc".to_string(),
//         };
//         heap.push(riws3.clone());
//         heap.push(riws4.clone());
//         assert_eq!(heap.peek().unwrap(), &riws1);
//         assert_eq!(riws8.cmp(&riws7), Ordering::Greater);

//         // compare with `doc`
//         let riws9 = RowIdWithScore {
//             row_id: 4,
//             score: 6.66,
//             seg_id: 2,
//             doc_id: 2,
//             doc: "abc".to_string(),
//         };
//         let riws10 = RowIdWithScore {
//             row_id: 4,
//             score: 6.66,
//             seg_id: 2,
//             doc_id: 2,
//             doc: "acd".to_string(),
//         };
//         heap.push(riws3.clone());
//         heap.push(riws4.clone());
//         assert_eq!(heap.peek().unwrap(), &riws1);
//         assert_eq!(riws10.cmp(&riws9), Ordering::Greater);

//         // compare `equal`
//         let riws11 = RowIdWithScore {
//             row_id: 4,
//             score: 1.11,
//             seg_id: 2,
//             doc_id: 2,
//             doc: "abc".to_string(),
//         };
//         let riws12 = RowIdWithScore {
//             row_id: 4,
//             score: 1.11,
//             seg_id: 2,
//             doc_id: 2,
//             doc: "abc".to_string(),
//         };
//         heap.push(riws11.clone());
//         heap.push(riws12.clone());
//         assert_eq!(heap.peek().unwrap(), &riws11);
//         assert_eq!(riws12.cmp(&riws11), Ordering::Equal);
//     }
// }
