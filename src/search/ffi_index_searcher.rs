// use super::index_searcher_core::*;
// use crate::cxx_vector_converter;
// use crate::logger::logger_bridge::TantivySearchLogger;
// use crate::RowIdWithScore;
// use crate::CXX_STRING_CONERTER;
// use crate::CXX_VECTOR_STRING_CONERTER;
// use crate::{common::constants::LOG_CALLBACK, ERROR};
// use cxx::CxxString;
// use cxx::CxxVector;

// pub fn ffi_load_index(index_path: &CxxString) -> bool {
//     let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
//         Ok(path) => path,
//         Err(e) => {
//             ERROR!(function: "ffi_load_index", "Can't convert 'index_path', message: {}", e);
//             return false;
//         }
//     };

//     match load_index(&index_path) {
//         Ok(status) => status,
//         Err(e) => {
//             ERROR!(function: "ffi_load_index", "Error loading index: {}", e);
//             false
//         }
//     }
// }

// pub fn ffi_free_reader(index_path: &CxxString) -> bool {
//     let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
//         Ok(path) => path,
//         Err(e) => {
//             ERROR!(function: "ffi_free_reader", "Can't convert 'index_path', message: {}", e);
//             return false;
//         }
//     };

//     match free_reader(&index_path) {
//         Ok(status) => status,
//         Err(e) => {
//             ERROR!(function: "ffi_free_reader", "Error freeing reader: {}", e);
//             false
//         }
//     }
// }

// pub fn ffi_search_in_rowid_range(
//     index_path: &CxxString,
//     column_name: &CxxString,
//     query: &CxxString,
//     lrange: u64,
//     rrange: u64,
//     use_regex: bool,
// ) -> bool {
//     let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
//         Ok(path) => path,
//         Err(e) => {
//             ERROR!(function: "ffi_search_in_rowid_range", "Can't convert 'index_path', message: {}", e);
//             return false;
//         }
//     };
//     let column_name: String = match CXX_STRING_CONERTER.convert(column_name) {
//         Ok(name) => name,
//         Err(e) => {
//             ERROR!(function: "ffi_search_in_rowid_range", "Can't convert 'column_name', message: {}", e);
//             return false;
//         }
//     };
//     let query: String = match CXX_STRING_CONERTER.convert(query) {
//         Ok(q) => q,
//         Err(e) => {
//             ERROR!(function: "ffi_search_in_rowid_range", "Can't convert 'query', message: {}", e);
//             return false;
//         }
//     };

//     let column_names: Vec<String> = vec![column_name];

//     match search_in_rowid_range(&index_path, &column_names, &query, lrange, rrange, use_regex) {
//         Ok(status) => status,
//         Err(e) => {
//             ERROR!(function: "ffi_search_in_rowid_range", "Error searching in rowid range: {}", e);
//             false
//         }
//     }
// }

// pub fn ffi_count_in_rowid_range(
//     index_path: &CxxString,
//     column_name: &CxxString,
//     query: &CxxString,
//     lrange: u64,
//     rrange: u64,
//     use_regex: bool,
// ) -> u64 {
//     let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
//         Ok(path) => path,
//         Err(e) => {
//             ERROR!(function: "ffi_count_in_rowid_range", "Can't convert 'index_path', message: {}", e);
//             return 0;
//         }
//     };
//     let column_name: String = match CXX_STRING_CONERTER.convert(column_name) {
//         Ok(name) => name,
//         Err(e) => {
//             ERROR!(function: "ffi_count_in_rowid_range", "Can't convert 'column_name', message: {}", e);
//             return 0;
//         }
//     };
//     let query: String = match CXX_STRING_CONERTER.convert(query) {
//         Ok(q) => q,
//         Err(e) => {
//             ERROR!(function: "ffi_count_in_rowid_range", "Can't convert 'query', message: {}", e);
//             return 0;
//         }
//     };

//     let column_names: Vec<String> = vec![column_name];

//     match count_in_rowid_range(&index_path, &column_names, &query, lrange, rrange, use_regex) {
//         Ok(count) => count,
//         Err(e) => {
//             ERROR!(function: "ffi_count_in_rowid_range", "Error counting in rowid range: {}", e);
//             0
//         }
//     }
// }

// pub fn ffi_bm25_search_with_filter(
//     index_path: &CxxString,
//     column_names: &CxxVector<CxxString>,
//     query: &CxxString,
//     u8_bitmap: &CxxVector<u8>,
//     top_k: u32,
//     need_text: bool,
// ) -> Vec<RowIdWithScore> {
//     let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
//         Ok(path) => path,
//         Err(e) => {
//             ERROR!(function: "ffi_bm25_search_with_filter", "Can't convert 'index_path', message: {}", e);
//             return Vec::new();
//         }
//     };

//     let column_names: Vec<String> = match CXX_VECTOR_STRING_CONERTER.convert(column_names) {
//         Ok(names) => names,
//         Err(e) => {
//             ERROR!(function: "ffi_bm25_search_with_filter", "Can't convert 'column_names', message: {}", e);
//             return Vec::new();
//         }
//     };

//     let query: String = match CXX_STRING_CONERTER.convert(query) {
//         Ok(q) => q,
//         Err(e) => {
//             ERROR!(function: "ffi_bm25_search_with_filter", "Can't convert 'query', message: {}", e);
//             return Vec::new();
//         }
//     };

//     let u8_bitmap: Vec<u8> = match cxx_vector_converter::<u8>().convert(u8_bitmap) {
//         Ok(bitmap) => bitmap,
//         Err(e) => {
//             ERROR!(function: "ffi_bm25_search_with_filter", "Can't convert vector 'u8_bitmap', message: {}", e);
//             return Vec::new();
//         }
//     };

//     match bm25_search_with_filter(&index_path, &column_names, &query, &u8_bitmap, top_k, need_text) {
//         Ok(results) => results,
//         Err(e) => {
//             ERROR!(function: "ffi_bm25_search_with_filter", "Error performing BM25 search with filter: {}", e);
//             Vec::new()
//         }
//     }
// }

// pub fn ffi_bm25_search(
//     index_path: &CxxString,
//     column_names: &CxxVector<CxxString>,
//     query: &CxxString,
//     top_k: u32,
//     need_text: bool,
// ) -> Vec<RowIdWithScore> {
//     let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
//         Ok(path) => path,
//         Err(e) => {
//             ERROR!(function: "ffi_bm25_search", "Can't convert 'index_path', message: {}", e);
//             return Vec::new();
//         }
//     };
//     let column_names: Vec<String> = match CXX_VECTOR_STRING_CONERTER.convert(column_names) {
//         Ok(names) => names,
//         Err(e) => {
//             ERROR!(function: "ffi_bm25_search", "Can't convert 'column_names', message: {}", e);
//             return Vec::new();
//         }
//     };
//     let query: String = match CXX_STRING_CONERTER.convert(query) {
//         Ok(q) => q,
//         Err(e) => {
//             ERROR!(function: "ffi_bm25_search", "Can't convert 'query', message: {}", e);
//             return Vec::new();
//         }
//     };

//     match bm25_search(&index_path, &column_names, &query, top_k, need_text) {
//         Ok(results) => results,
//         Err(e) => {
//             ERROR!(function: "ffi_bm25_search", "Error performing BM25 search: {}", e);
//             Vec::new()
//         }
//     }
// }

// pub fn ffi_search_bitmap_results(
//     index_path: &CxxString,
//     column_names: &CxxVector<CxxString>,
//     query: &CxxString,
//     use_regex: bool,
// ) -> Vec<u8> {
//     let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
//         Ok(path) => path,
//         Err(e) => {
//             ERROR!(function: "ffi_search_bitmap_results", "Can't convert 'index_path', message: {}", e);
//             return Vec::new();
//         }
//     };
//     let column_names: Vec<String> = match CXX_VECTOR_STRING_CONERTER.convert(column_names) {
//         Ok(names) => names,
//         Err(e) => {
//             ERROR!(function: "ffi_search_bitmap_results", "Can't convert 'column_names', message: {}", e);
//             return Vec::new();
//         }
//     };
//     let query: String = match CXX_STRING_CONERTER.convert(query) {
//         Ok(q) => q,
//         Err(e) => {
//             ERROR!(function: "ffi_search_bitmap_results", "Can't convert 'query', message: {}", e);
//             return Vec::new();
//         }
//     };

//     match search_bitmap_results(&index_path, &column_names, &query, use_regex) {
//         Ok(results) => results,
//         Err(e) => {
//             ERROR!(function: "ffi_search_bitmap_results", "Error searching bitmap results: {}", e);
//             Vec::new()
//         }
//     }
// }

// pub fn ffi_indexed_doc_counts(index_path: &CxxString) -> u64 {
//     let index_path: String = match CXX_STRING_CONERTER.convert(index_path) {
//         Ok(path) => path,
//         Err(e) => {
//             ERROR!(function: "ffi_indexed_doc_counts", "Can't convert 'index_path', message: {}", e);
//             return 0;
//         }
//     };
//     match indexed_doc_counts(&index_path) {
//         Ok(count) => count,
//         Err(e) => {
//             ERROR!(function: "ffi_indexed_doc_counts", "Error getting indexed doc counts: {}", e);
//             0
//         }
//     }
// }
