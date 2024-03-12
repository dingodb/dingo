// #[cfg(test)]
// mod tests {
//     use std::fs;

//     use cxx::{let_cxx_string, CxxVector};
//     use log::LevelFilter;
//     use tantivy::merge_policy::LogMergePolicy;
//     use tantivy::schema::{Schema, FAST, INDEXED, STORED, TEXT};
//     use tantivy::{Document, Index};
//     use tempfile::TempDir;

//     use crate::{
//         tantivy_bm25_search, tantivy_bm25_search_with_filter, tantivy_count_in_rowid_range,
//         tantivy_indexed_doc_counts, tantivy_load_index, tantivy_reader_free,
//         tantivy_search_bitmap_results, tantivy_search_in_rowid_range, update_logger_for_test,
//     };

//     fn commit_some_docs_for_test(index_directory: String, need_store_doc: bool) {
//         update_logger_for_test(LevelFilter::Debug);
//         // Construct the schema for the index.
//         let mut schema_builder = Schema::builder();
//         schema_builder.add_u64_field("row_id", FAST | INDEXED);
//         if need_store_doc {
//             schema_builder.add_text_field("text", TEXT | STORED);
//         } else {
//             schema_builder.add_text_field("text", TEXT);
//         }
//         let schema = schema_builder.build();
//         // Create the index in the specified directory.
//         let index = Index::create_in_dir(index_directory.clone(), schema.clone())
//             .expect("Can't create index");
//         // Create the writer with a specified buffer size (e.g., 64 MB).
//         let mut writer = index
//             .writer_with_num_threads(2, 1024 * 1024 * 64)
//             .expect("Can't create index writer");
//         // Configure default merge policy.
//         writer.set_merge_policy(Box::new(LogMergePolicy::default()));

//         // Get fields from `schema`.
//         let row_id_field = index
//             .schema()
//             .get_field("row_id")
//             .expect("Can't get row_id filed");
//         let text_field = index
//             .schema()
//             .get_field("text")
//             .expect("Can't get text filed");

//         // Index some documents.
//         let docs: Vec<String> = vec![
//             "Ancient empires rise and fall, shaping history's course.".to_string(),
//             "Artistic expressions reflect diverse cultural heritages.".to_string(),
//             "Social movements transform societies, forging new paths.".to_string(),
//             "Strategic military campaigns alter the balance of power.".to_string(),
//             "Ancient philosophies provide wisdom for modern dilemmas.".to_string(),
//         ];
//         for row_id in 0..docs.len() {
//             let mut doc = Document::default();
//             doc.add_u64(row_id_field, row_id as u64);
//             doc.add_text(text_field, &docs[row_id]);
//             let result = writer.add_document(doc);
//             assert!(result.is_ok());
//         }
//         assert!(writer.commit().is_ok());
//         assert!(writer.wait_merging_threads().is_ok());
//     }

//     #[test]
//     fn test_load_index() {
//         let temp_path = TempDir::new().unwrap();
//         let temp_path_str = temp_path.path().to_str().unwrap();
//         let_cxx_string!(temp_path_cxx = temp_path_str);
//         let index_directory = temp_path_cxx.as_ref().get_ref();
//         let_cxx_string!(query = "Ancient");
//         let query_cxx = query.as_ref().get_ref();

//         commit_some_docs_for_test(temp_path_str.to_string(), false);

//         // Before load reader, trying search.
//         let result = tantivy_count_in_rowid_range(index_directory, query_cxx, 0, 100, false);
//         assert!(result.is_err());

//         assert!(tantivy_load_index(index_directory).is_ok());

//         // After load reader, trying search.
//         let result = tantivy_count_in_rowid_range(index_directory, query_cxx, 0, 100, false);
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), 2);
//     }

//     #[test]
//     fn test_load_index_boundary_1() {
//         // load index with not exist directory
//         let not_exist = TempDir::new().unwrap();
//         fs::remove_dir_all(not_exist.path()).unwrap();
//         assert_eq!(not_exist.path().exists(), false);
//         let_cxx_string!(not_exist_cxx = "not_exist");
//         let not_exist_cxx = not_exist_cxx.as_ref().get_ref();
//         assert!(tantivy_load_index(not_exist_cxx).is_err());
//     }

//     #[test]
//     fn test_load_index_boundary_2() {
//         // load index with empty directory
//         let temp_path = TempDir::new().unwrap();
//         let_cxx_string!(empty_path = temp_path.path().to_str().unwrap());
//         let empty_path_cxx = empty_path.as_ref().get_ref();
//         assert!(tantivy_load_index(empty_path_cxx).is_err());
//     }

//     #[test]
//     fn test_reader_free() {
//         let temp_path = TempDir::new().unwrap();
//         let temp_path_str = temp_path.path().to_str().unwrap();
//         let_cxx_string!(temp_path_cxx = temp_path_str);
//         let index_directory = temp_path_cxx.as_ref().get_ref();
//         let_cxx_string!(query = "Ancient");
//         let query_cxx = query.as_ref().get_ref();

//         // Index some docs and load index reader.
//         commit_some_docs_for_test(temp_path_str.to_string(), false);
//         assert!(tantivy_load_index(index_directory).is_ok());

//         // Before free reader, trying search.
//         let result = tantivy_count_in_rowid_range(index_directory, query_cxx, 0, 100, false);
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), 2);
//         assert!(tantivy_reader_free(index_directory).is_ok());

//         // After free reader, trying search.
//         let result = tantivy_count_in_rowid_range(index_directory, query_cxx, 0, 100, false);
//         assert!(result.is_err());
//     }

//     #[test]
//     fn test_reader_free_boundary_1() {
//         // reader free with not exist directory
//         let not_exist = TempDir::new().unwrap();
//         fs::remove_dir_all(not_exist.path()).unwrap();
//         assert_eq!(not_exist.path().exists(), false);
//         let_cxx_string!(not_exist_cxx = "not_exist");
//         let not_exist_cxx = not_exist_cxx.as_ref().get_ref();
//         let free_result = tantivy_reader_free(not_exist_cxx);
//         assert!(free_result.is_ok());
//         assert_eq!(free_result.unwrap(), false);
//     }

//     #[test]
//     fn test_reader_free_boundary_2() {
//         // reader free with empty directory
//         let temp_path = TempDir::new().unwrap();
//         let_cxx_string!(empty_path = temp_path.path().to_str().unwrap());
//         let empty_path_cxx = empty_path.as_ref().get_ref();
//         let free_result = tantivy_reader_free(empty_path_cxx);
//         assert!(free_result.is_ok());
//         assert_eq!(free_result.unwrap(), false);
//     }

//     #[test]
//     fn test_search_in_rowid_range() {
//         let temp_path = TempDir::new().unwrap();
//         let temp_path_str = temp_path.path().to_str().unwrap();
//         let_cxx_string!(temp_path_cxx = temp_path_str);
//         let index_directory = temp_path_cxx.as_ref().get_ref();
//         let_cxx_string!(normal_query = "Ancient");
//         let normal_query_cxx = normal_query.as_ref().get_ref();
//         let_cxx_string!(like_query = "%cien%");
//         let like_query_cxx = like_query.as_ref().get_ref();

//         // Index some docs and load index reader.
//         commit_some_docs_for_test(temp_path_str.to_string(), false);
//         assert!(tantivy_load_index(index_directory).is_ok());

//         // Before free reader, trying search.
//         let result = tantivy_search_in_rowid_range(index_directory, normal_query_cxx, 0, 1, false);
//         assert_eq!(result.unwrap(), true);
//         let result = tantivy_search_in_rowid_range(index_directory, normal_query_cxx, 1, 2, false);
//         assert_eq!(result.unwrap(), false);
//         let result = tantivy_search_in_rowid_range(index_directory, like_query_cxx, 0, 1, true);
//         assert_eq!(result.unwrap(), true);
//         let result = tantivy_search_in_rowid_range(index_directory, like_query_cxx, 1, 2, true);
//         assert_eq!(result.unwrap(), false);
//         let result = tantivy_search_in_rowid_range(index_directory, normal_query_cxx, 0, 0, false);
//         assert_eq!(result.unwrap(), true);
//         let result = tantivy_search_in_rowid_range(index_directory, normal_query_cxx, 1, 1, false);
//         assert_eq!(result.unwrap(), false);
//         let result = tantivy_search_in_rowid_range(index_directory, like_query_cxx, 0, 0, true);
//         assert_eq!(result.unwrap(), true);
//         let result = tantivy_search_in_rowid_range(index_directory, like_query_cxx, 1, 1, true);
//         assert_eq!(result.unwrap(), false);
//         let result =
//             tantivy_search_in_rowid_range(index_directory, normal_query_cxx, 4, 800, false);
//         assert_eq!(result.unwrap(), true);
//         // `use_regex` works as a hashmap key.
//         let result = tantivy_search_in_rowid_range(index_directory, like_query_cxx, 4, 800, false);
//         assert_eq!(result.unwrap(), false);
//     }

//     #[test]
//     fn test_search_in_rowid_range_boundary_1() {
//         // execute search with empty directory
//         let temp_path = TempDir::new().unwrap();
//         let temp_path_str = temp_path.path().to_str().unwrap();
//         let_cxx_string!(temp_path_cxx = temp_path_str);
//         let empty_path_cxx = temp_path_cxx.as_ref().get_ref();
//         let_cxx_string!(normal_query = "Ancient");
//         let normal_query_cxx = normal_query.as_ref().get_ref();
//         let search_result =
//             tantivy_search_in_rowid_range(empty_path_cxx, normal_query_cxx, 0, 100, false);
//         assert!(search_result.is_err());
//     }

//     #[test]
//     fn test_search_in_rowid_range_boundary_2() {
//         // execute search with invalid rowid range
//         let temp_path = TempDir::new().unwrap();
//         let temp_path_str = temp_path.path().to_str().unwrap();
//         let_cxx_string!(temp_path_cxx = temp_path_str);
//         let index_directory_cxx = temp_path_cxx.as_ref().get_ref();
//         let_cxx_string!(normal_query = "Ancient");
//         let normal_query_cxx = normal_query.as_ref().get_ref();

//         // Index some docs and load index reader.
//         commit_some_docs_for_test(temp_path_str.to_string(), false);
//         assert!(tantivy_load_index(index_directory_cxx).is_ok());

//         let search_result =
//             tantivy_search_in_rowid_range(index_directory_cxx, normal_query_cxx, 100, 10, false);
//         assert!(search_result.is_err());
//     }

//     #[test]
//     fn test_search_in_rowid_range_boundary_3() {
//         // execute search with not exist directory
//         let not_exist = TempDir::new().unwrap();
//         fs::remove_dir_all(not_exist.path()).unwrap();
//         assert_eq!(not_exist.path().exists(), false);
//         let_cxx_string!(not_exist_cxx = "not_exist");
//         let not_exist_cxx = not_exist_cxx.as_ref().get_ref();
//         let_cxx_string!(normal_query = "Ancient");
//         let normal_query_cxx = normal_query.as_ref().get_ref();
//         let search_result =
//             tantivy_search_in_rowid_range(not_exist_cxx, normal_query_cxx, 0, 100, false);
//         assert!(search_result.is_err());
//     }

//     #[test]
//     fn test_count_in_rowid_range() {
//         let temp_path = TempDir::new().unwrap();
//         let temp_path_str = temp_path.path().to_str().unwrap();
//         let_cxx_string!(temp_path_cxx = temp_path_str);
//         let index_directory = temp_path_cxx.as_ref().get_ref();
//         let_cxx_string!(normal_query = "Ancient");
//         let normal_query_cxx = normal_query.as_ref().get_ref();
//         let_cxx_string!(like_query = "%cien%");
//         let like_query_cxx = like_query.as_ref().get_ref();

//         // Index some docs and load index reader.
//         commit_some_docs_for_test(temp_path_str.to_string(), false);
//         assert!(tantivy_load_index(index_directory).is_ok());

//         // Before free reader, trying search.
//         let result =
//             tantivy_count_in_rowid_range(index_directory, normal_query_cxx, 0, 1000, false);
//         assert_eq!(result.unwrap(), 2);
//         let result = tantivy_count_in_rowid_range(index_directory, like_query_cxx, 0, 1000, true);
//         assert_eq!(result.unwrap(), 2);
//         let result = tantivy_count_in_rowid_range(index_directory, normal_query_cxx, 0, 1, false);
//         assert_eq!(result.unwrap(), 1);
//         let result = tantivy_count_in_rowid_range(index_directory, normal_query_cxx, 1, 2, false);
//         assert_eq!(result.unwrap(), 0);
//         let result = tantivy_count_in_rowid_range(index_directory, like_query_cxx, 0, 1, true);
//         assert_eq!(result.unwrap(), 1);
//         let result = tantivy_count_in_rowid_range(index_directory, like_query_cxx, 1, 2, true);
//         assert_eq!(result.unwrap(), 0);
//         let result = tantivy_count_in_rowid_range(index_directory, normal_query_cxx, 0, 0, false);
//         assert_eq!(result.unwrap(), 1);
//         let result = tantivy_count_in_rowid_range(index_directory, normal_query_cxx, 1, 1, false);
//         assert_eq!(result.unwrap(), 0);
//         let result = tantivy_count_in_rowid_range(index_directory, like_query_cxx, 0, 0, true);
//         assert_eq!(result.unwrap(), 1);
//         let result = tantivy_count_in_rowid_range(index_directory, like_query_cxx, 1, 1, true);
//         assert_eq!(result.unwrap(), 0);
//         let result = tantivy_count_in_rowid_range(index_directory, normal_query_cxx, 4, 800, false);
//         assert_eq!(result.unwrap(), 1);
//         // `use_regex` works as a hashmap key.
//         let result = tantivy_count_in_rowid_range(index_directory, like_query_cxx, 4, 800, false);
//         assert_eq!(result.unwrap(), 0);
//     }

//     #[test]
//     fn test_count_in_rowid_range_boundary_1() {
//         // execute search with empty directory
//         let temp_path = TempDir::new().unwrap();
//         let temp_path_str = temp_path.path().to_str().unwrap();
//         let_cxx_string!(temp_path_cxx = temp_path_str);
//         let empty_path_cxx = temp_path_cxx.as_ref().get_ref();
//         let_cxx_string!(normal_query = "Ancient");
//         let normal_query_cxx = normal_query.as_ref().get_ref();
//         let search_result =
//             tantivy_count_in_rowid_range(empty_path_cxx, normal_query_cxx, 0, 100, false);
//         assert!(search_result.is_err());
//     }

//     #[test]
//     fn test_count_in_rowid_range_boundary_2() {
//         // execute search with invalid rowid range
//         let temp_path = TempDir::new().unwrap();
//         let temp_path_str = temp_path.path().to_str().unwrap();
//         let_cxx_string!(temp_path_cxx = temp_path_str);
//         let index_directory_cxx = temp_path_cxx.as_ref().get_ref();
//         let_cxx_string!(normal_query = "Ancient");
//         let normal_query_cxx = normal_query.as_ref().get_ref();

//         // Index some docs and load index reader.
//         commit_some_docs_for_test(temp_path_str.to_string(), false);
//         assert!(tantivy_load_index(index_directory_cxx).is_ok());

//         let search_result =
//             tantivy_count_in_rowid_range(index_directory_cxx, normal_query_cxx, 100, 10, false);
//         assert!(search_result.is_err());
//     }

//     #[test]
//     fn test_count_in_rowid_range_boundary_3() {
//         // execute search with not exist directory
//         let not_exist = TempDir::new().unwrap();
//         fs::remove_dir_all(not_exist.path()).unwrap();
//         assert_eq!(not_exist.path().exists(), false);
//         let_cxx_string!(not_exist_cxx = "not_exist");
//         let not_exist_cxx = not_exist_cxx.as_ref().get_ref();
//         let_cxx_string!(normal_query = "Ancient");
//         let normal_query_cxx = normal_query.as_ref().get_ref();
//         let search_result =
//             tantivy_count_in_rowid_range(not_exist_cxx, normal_query_cxx, 0, 100, false);
//         assert!(search_result.is_err());
//     }

//     #[test]
//     fn test_bm25_search_with_filter() {
//         let temp_path = TempDir::new().unwrap();
//         let temp_path_str = temp_path.path().to_str().unwrap();
//         let_cxx_string!(temp_path_cxx = temp_path_str);
//         let index_directory = temp_path_cxx.as_ref().get_ref();
//         let_cxx_string!(query = "Ancient");
//         let query_cxx = query.as_ref().get_ref();

//         // Index some docs and load index reader.
//         commit_some_docs_for_test(temp_path_str.to_string(), false);
//         assert!(tantivy_load_index(index_directory).is_ok());

//         // Create a `u8_bitmap_cxx` represent alive row_ids (row_id: 0, 4).
//         let mut u8_bitmap_cxx1: cxx::UniquePtr<CxxVector<u8>> = CxxVector::new();
//         if let Some(mut pinned_vector) = u8_bitmap_cxx1.as_mut() {
//             pinned_vector.as_mut().push(17); // 0001 0001
//         }

//         // Create a `u8_bitmap_cxx` represent alive row_ids (row_id: 0).
//         let mut u8_bitmap_cxx2: cxx::UniquePtr<CxxVector<u8>> = CxxVector::new();
//         if let Some(mut pinned_vector) = u8_bitmap_cxx2.as_mut() {
//             pinned_vector.as_mut().push(1); // 0001 0001
//         }

//         let result =
//             tantivy_bm25_search_with_filter(index_directory, query_cxx, &u8_bitmap_cxx1, 10, false)
//                 .unwrap();
//         assert_eq!(result.len(), 2);
//         assert_eq!(result[0].row_id, 4);
//         assert_eq!(result[1].row_id, 0);
//         assert_eq!(result[0].score, 0.8952658);
//         assert_eq!(result[1].score, 0.80432457);

//         let result =
//             tantivy_bm25_search_with_filter(index_directory, query_cxx, &u8_bitmap_cxx2, 10, false)
//                 .unwrap();
//         assert_eq!(result.len(), 1);
//         assert_eq!(result[0].row_id, 0);
//         assert_eq!(result[0].score, 0.80432457);

//         let result =
//             tantivy_bm25_search_with_filter(index_directory, query_cxx, &u8_bitmap_cxx1, 0, false)
//                 .unwrap();
//         assert_eq!(result.len(), 0);
//     }

//     #[test]
//     fn test_bm25_search_with_filter_boundary_1() {
//         // Doesn't store origin doc contenct when build index.
//         let temp_path = TempDir::new().unwrap();
//         let temp_path_str = temp_path.path().to_str().unwrap();
//         let_cxx_string!(temp_path_cxx = temp_path_str);
//         let index_directory = temp_path_cxx.as_ref().get_ref();
//         let_cxx_string!(query = "Ancient");
//         let query_cxx = query.as_ref().get_ref();

//         // Index some docs and load index reader.
//         commit_some_docs_for_test(temp_path_str.to_string(), false);
//         assert!(tantivy_load_index(index_directory).is_ok());

//         // Create a `u8_bitmap_cxx` represent alive row_ids (row_id: 0, 4).
//         let mut u8_bitmap_cxx: cxx::UniquePtr<CxxVector<u8>> = CxxVector::new();
//         if let Some(mut pinned_vector) = u8_bitmap_cxx.as_mut() {
//             pinned_vector.as_mut().push(17); // 0001 0001
//         }

//         let result =
//             tantivy_bm25_search_with_filter(index_directory, query_cxx, &u8_bitmap_cxx, 10, false);
//         assert!(result.is_ok());

//         let result =
//             tantivy_bm25_search_with_filter(index_directory, query_cxx, &u8_bitmap_cxx, 10, true);
//         assert!(result.is_err());
//     }

//     #[test]
//     fn test_bm25_search_with_filter_boundary_2() {
//         // Store origin doc contenct when build index.
//         let temp_path = TempDir::new().unwrap();
//         let temp_path_str = temp_path.path().to_str().unwrap();
//         let_cxx_string!(temp_path_cxx = temp_path_str);
//         let index_directory = temp_path_cxx.as_ref().get_ref();
//         let_cxx_string!(query = "Ancient");
//         let query_cxx = query.as_ref().get_ref();

//         // Index some docs and load index reader.
//         commit_some_docs_for_test(temp_path_str.to_string(), true);
//         assert!(tantivy_load_index(index_directory).is_ok());

//         // Create a `u8_bitmap_cxx` represent alive row_ids (row_id: 0, 4).
//         let mut u8_bitmap_cxx: cxx::UniquePtr<CxxVector<u8>> = CxxVector::new();
//         if let Some(mut pinned_vector) = u8_bitmap_cxx.as_mut() {
//             pinned_vector.as_mut().push(17); // 0001 0001
//         }

//         let result =
//             tantivy_bm25_search_with_filter(index_directory, query_cxx, &u8_bitmap_cxx, 10, false);
//         assert!(result.is_ok());

//         let result =
//             tantivy_bm25_search_with_filter(index_directory, query_cxx, &u8_bitmap_cxx, 10, true);
//         assert_eq!(
//             result.unwrap()[0].doc,
//             "Ancient philosophies provide wisdom for modern dilemmas."
//         );
//     }

//     #[test]
//     fn test_bm25_search_with_filter_boundary_3() {
//         // Search with an empty directory
//         let temp_path = TempDir::new().unwrap();
//         let temp_path_str = temp_path.path().to_str().unwrap();
//         let_cxx_string!(temp_path_cxx = temp_path_str);
//         let empty_path_cxx = temp_path_cxx.as_ref().get_ref();
//         let_cxx_string!(query = "Ancient");
//         let query_cxx = query.as_ref().get_ref();

//         let u8_bitmap_cxx: cxx::UniquePtr<CxxVector<u8>> = CxxVector::new();
//         let result =
//             tantivy_bm25_search_with_filter(empty_path_cxx, query_cxx, &u8_bitmap_cxx, 10, false);
//         assert!(result.is_err());
//     }

//     #[test]
//     fn test_bm25_search_with_filter_boundary_4() {
//         // execute search with not exist directory
//         let not_exist = TempDir::new().unwrap();
//         fs::remove_dir_all(not_exist.path()).unwrap();
//         assert_eq!(not_exist.path().exists(), false);
//         let_cxx_string!(not_exist_cxx = "not_exist");
//         let not_exist_cxx = not_exist_cxx.as_ref().get_ref();
//         let_cxx_string!(query = "Ancient");
//         let query_cxx = query.as_ref().get_ref();

//         let u8_bitmap_cxx: cxx::UniquePtr<CxxVector<u8>> = CxxVector::new();
//         let result =
//             tantivy_bm25_search_with_filter(not_exist_cxx, query_cxx, &u8_bitmap_cxx, 10, false);
//         assert!(result.is_err());
//     }

//     #[test]
//     fn test_bm25_search() {
//         let temp_path = TempDir::new().unwrap();
//         let temp_path_str = temp_path.path().to_str().unwrap();
//         let_cxx_string!(temp_path_cxx = temp_path_str);
//         let index_directory = temp_path_cxx.as_ref().get_ref();
//         let_cxx_string!(query = "Ancient");
//         let query_cxx = query.as_ref().get_ref();

//         // Index some docs and load index reader.
//         commit_some_docs_for_test(temp_path_str.to_string(), false);
//         assert!(tantivy_load_index(index_directory).is_ok());

//         let result = tantivy_bm25_search(index_directory, query_cxx, 10, false).unwrap();
//         assert_eq!(result.len(), 2);
//         assert_eq!(result[0].row_id, 4);
//         assert_eq!(result[1].row_id, 0);
//         assert_eq!(result[0].score, 0.8952658);
//         assert_eq!(result[1].score, 0.80432457);

//         let result = tantivy_bm25_search(index_directory, query_cxx, 1, false).unwrap();
//         assert_eq!(result.len(), 1);
//         assert_eq!(result[0].row_id, 4);
//         assert_eq!(result[0].score, 0.8952658);

//         let result = tantivy_bm25_search(index_directory, query_cxx, 0, false).unwrap();
//         assert_eq!(result.len(), 0);
//     }

//     #[test]
//     fn test_search_bitmap_results() {
//         let temp_path = TempDir::new().unwrap();
//         let temp_path_str = temp_path.path().to_str().unwrap();
//         let_cxx_string!(temp_path_cxx = temp_path_str);
//         let index_directory = temp_path_cxx.as_ref().get_ref();
//         let_cxx_string!(normal_query = "Ancient"); // 0001 0001
//         let normal_query_cxx = normal_query.as_ref().get_ref();
//         let_cxx_string!(like_query = "%for%"); // 0001 0100
//         let like_query_cxx = like_query.as_ref().get_ref();

//         // Index some docs and load index reader.
//         commit_some_docs_for_test(temp_path_str.to_string(), false);
//         assert!(tantivy_load_index(index_directory).is_ok());

//         let result =
//             tantivy_search_bitmap_results(index_directory, normal_query_cxx, false).unwrap();
//         assert_eq!(result, vec![17]);

//         let result = tantivy_search_bitmap_results(index_directory, like_query_cxx, true).unwrap();
//         assert_eq!(result, vec![20]);
//     }

//     #[test]
//     fn test_search_bitmap_results_boundary_1() {
//         // execute search with not exist directory
//         let not_exist = TempDir::new().unwrap();
//         fs::remove_dir_all(not_exist.path()).unwrap();
//         assert_eq!(not_exist.path().exists(), false);
//         let_cxx_string!(not_exist_cxx = "not_exist");
//         let not_exist_cxx = not_exist_cxx.as_ref().get_ref();
//         let_cxx_string!(query = "Ancient");
//         let query_cxx = query.as_ref().get_ref();

//         let result = tantivy_search_bitmap_results(not_exist_cxx, query_cxx, false);
//         assert!(result.is_err());
//     }

//     #[test]
//     fn test_search_bitmap_results_boundary_2() {
//         // Search with an empty directory
//         let temp_path = TempDir::new().unwrap();
//         let temp_path_str = temp_path.path().to_str().unwrap();
//         let_cxx_string!(temp_path_cxx = temp_path_str);
//         let empty_path_cxx = temp_path_cxx.as_ref().get_ref();
//         let_cxx_string!(query = "Ancient");
//         let query_cxx = query.as_ref().get_ref();

//         let result = tantivy_search_bitmap_results(empty_path_cxx, query_cxx, false);
//         assert!(result.is_err());
//     }

//     #[test]
//     fn test_tantivy_indexed_doc_counts() {
//         let temp_path = TempDir::new().unwrap();
//         let temp_path_str = temp_path.path().to_str().unwrap();
//         let_cxx_string!(temp_path_cxx = temp_path_str);
//         let index_directory = temp_path_cxx.as_ref().get_ref();

//         // Index some docs and load index reader.
//         commit_some_docs_for_test(temp_path_str.to_string(), false);
//         assert!(tantivy_load_index(index_directory).is_ok());

//         assert_eq!(tantivy_indexed_doc_counts(index_directory).unwrap(), 5);
//     }

//     #[test]
//     fn test_tantivy_indexed_doc_counts_boundary_1() {
//         let temp_path = TempDir::new().unwrap();
//         let temp_path_str = temp_path.path().to_str().unwrap();
//         let_cxx_string!(temp_path_cxx = temp_path_str);
//         let index_directory = temp_path_cxx.as_ref().get_ref();

//         // Index some docs and doesn't load index reader.
//         commit_some_docs_for_test(temp_path_str.to_string(), false);

//         assert_eq!(tantivy_indexed_doc_counts(index_directory).unwrap(), 0);
//     }

//     #[test]
//     fn test_tantivy_indexed_doc_counts_boundary_2() {
//         let temp_path = TempDir::new().unwrap();
//         let temp_path_str = temp_path.path().to_str().unwrap();
//         let_cxx_string!(temp_path_cxx = temp_path_str);
//         let index_directory = temp_path_cxx.as_ref().get_ref();

//         assert_eq!(tantivy_indexed_doc_counts(index_directory).unwrap(), 0);
//     }
// }
