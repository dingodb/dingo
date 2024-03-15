use roaring::RoaringBitmap;
use std::sync::Arc;
use crate::common::errors::IndexSearcherError;


pub struct FFiIndexSearcherUtils;

impl FFiIndexSearcherUtils {
    pub fn intersect_with_range(
        rowid_bitmap: Arc<RoaringBitmap>,
        lrange: u64,
        rrange: u64,
    ) -> Result<Arc<RoaringBitmap>, IndexSearcherError> {
        let lrange_u32: u32 = lrange
            .try_into()
            .map_err(|_|IndexSearcherError::BitmapOverflowError("`lrange > u32`".to_string()))?;
        let rrange_u32: u32 = rrange
            .try_into()
            .map_err(|_|IndexSearcherError::BitmapOverflowError("`rrange > u32`".to_string()))?;
        let rrange_plus_one_u32 = rrange_u32
            .checked_add(1)
            .ok_or(IndexSearcherError::BitmapOverflowError("`rrange+1` > `u32`".to_string()))?;

        let mut row_id_range = RoaringBitmap::new();
        row_id_range.insert_range(lrange_u32..rrange_plus_one_u32);
        row_id_range &= Arc::as_ref(&rowid_bitmap);
        Ok(Arc::new(row_id_range))
    }
    

    // fn compute_bitmap(
    //     index_reader_bridge: &IndexReaderBridge,
    //     column_names: &Vec<String>,
    //     query_str: &str,
    //     use_regex: bool,
    // ) -> Result<Arc<RoaringBitmap>, IndexSearcherError> {
    //     let schema: Schema = index_reader_bridge.reader.searcher().index().schema();

    //     let fields: Result<Vec<Field>, TantivyError> = column_names.iter().map(|column_name|{
    //         schema.get_field(column_name.as_str())
    //     }).collect();

    //     let mut fields: Vec<Field> = fields.map_err(|e|{
    //         let error = IndexSearcherError::TantivyError(e);
    //         ERROR!(function:"compute_bitmap", "{}", error);
    //         error
    //     })?;

    //     if fields.len()==0 {
    //         fields = schema.fields().filter(|(field,_)|{
    //             schema.get_field_name(*field)!="row_id"
    //         }).map(|(field, _)| field).collect();
    //     }

    //     if fields.len()==0 {
    //         let error = IndexSearcherError::EmptyFieldsError;
    //         ERROR!(function:"compute_bitmap", "{}", error);
    //         return Err(error);
    //     }

    //     INFO!(function:"compute_bitmap", "fields:{:?}", fields);


    //     let searcher: Searcher = index_reader_bridge.reader.searcher();
    //     let row_id_collector: RowIdRoaringCollector = RowIdRoaringCollector::with_field("row_id".to_string());

    //     if use_regex {
    //         if fields.len()!=1 {
    //             let error = IndexSearcherError::InternalError("Can't execute regex query in multi fields.".to_string());
    //             ERROR!(function:"compute_bitmap", "{}", error);
    //             return Err(error);
    //         }

    //         let regex_query = RegexQuery::from_pattern(&ConvertUtils::like_to_regex(query_str), fields[0]).map_err(|e|{
    //             ERROR!(function:"compute_bitmap", "Error when parse regex query:{}. {}", ConvertUtils::like_to_regex(query_str), e);
    //             IndexSearcherError::TantivyError(e)
    //         })?;

    //         let searched_bitmap = searcher.search(&regex_query, &row_id_collector).map_err(|e|{
    //             ERROR!(function:"compute_bitmap", "Error when execute regex query:{}. {}", ConvertUtils::like_to_regex(query_str), e);
    //             IndexSearcherError::TantivyError(e)
    //         })?;

    //         Ok(searched_bitmap)
    //     } else {
    //         let query_parser =
    //             QueryParser::for_index(index_reader_bridge.reader.searcher().index(), fields);

    //         let text_query = query_parser.parse_query(query_str).map_err(|e|{
    //             let error = IndexSearcherError::QueryParserError(e.to_string());
    //             ERROR!(function:"compute_bitmap", "{}", error);
    //             error
    //         })?;

    //         searcher.search(&text_query, &row_id_collector).map_err(|e|{
    //             ERROR!(function:"compute_bitmap", "Error when execute query:{}. {}", query_str, e);
    //             IndexSearcherError::TantivyError(e)
    //         })
    //     }
    // }

    // fn intersect_and_return(
    //     row_id_roaring_bitmap: Arc<RoaringBitmap>,
    //     lrange: u64,
    //     rrange: u64,
    // ) -> Result<Arc<RoaringBitmap>, IndexSearcherError> {
    //     let lrange_u32: u32 = lrange
    //         .try_into()
    //         .map_err(|_|IndexSearcherError::BitmapOverflowError("`lrange > u32`".to_string()))?;
    //     let rrange_u32: u32 = rrange
    //         .try_into()
    //         .map_err(|_|IndexSearcherError::BitmapOverflowError("`rrange > u32`".to_string()))?;
    //     let rrange_plus_one_u32 = rrange_u32
    //         .checked_add(1)
    //         .ok_or(IndexSearcherError::BitmapOverflowError("`rrange+1` > `u32`".to_string()))?;

    //     let mut row_id_range = RoaringBitmap::new();
    //     row_id_range.insert_range(lrange_u32..rrange_plus_one_u32);
    //     row_id_range &= Arc::as_ref(&row_id_roaring_bitmap);
    //     Ok(Arc::new(row_id_range))
    // }

    // /// Performs a search operation using the given index reader, query, and range.
    // ///
    // /// Arguments:
    // /// - `index_r`: refrence to `IndexReaderBridge`.
    // /// - `query_str`: query string.
    // /// - `lrange`: The lower bound of the row ID range.
    // /// - `rrange`: The upper bound of the row ID range.
    // /// - `use_regex`: Whether to use regex search.
    // ///
    // /// Returns:
    // /// - `Result<RoaringBitmap, String>`: A `RoaringBitmap` containing the search results,
    // ///   or an error if the search fails.
    // pub fn perform_search_with_range(
    //     index_r: &IndexReaderBridge,
    //     column_names: &Vec<String>,
    //     query_str: &str,
    //     lrange: u64,
    //     rrange: u64,
    //     use_regex: bool,
    // ) -> Result<Arc<RoaringBitmap>, IndexSearcherError> {
    //     let row_ids = Self::perform_search(index_r, column_names, query_str, use_regex)?;
    //     Self::intersect_and_return(row_ids, lrange, rrange)
    // }

    // pub fn perform_search(
    //     index_r: &IndexReaderBridge,
    //     column_name: &str,
    //     query_str: &str,
    //     use_regex: bool,
    // ) -> Result<Arc<RoaringBitmap>, IndexSearcherError> {
    //     #[cfg(feature = "use-flurry-cache")]
    //     {
    //         // Resolve cache or compute the roaring bitmap for the given query.
    //         CACHE_FOR_SKIP_INDEX.resolve(
    //             (
    //                 index_r.reader_address(),
    //                 query_str.to_string(),
    //                 index_r.path.to_string(),
    //                 use_regex,
    //             ),
    //             || Self::compute_bitmap(index_r, column_names, query_str, use_regex),
    //         )
    //     }
    //     #[cfg(not(feature = "use-flurry-cache"))]
    //     {
    //         Self::compute_bitmap(index_r, column_names, query_str, use_regex)
    //     }
    // }
}





// #[cfg(test)]
// mod tests {
//     mod ffi_index_searcher_utils {
//         use tantivy::{
//             merge_policy::LogMergePolicy,
//             schema::{Schema, FAST, INDEXED, STORED, TEXT},
//             Document, Index, ReloadPolicy,
//         };
//         use tempfile::TempDir;

//         use super::super::*;

//         fn index_some_docs_in_temp_directory(index_directory_str: &str) -> IndexReaderBridge {
//             // Construct the schema for the index.
//             let mut schema_builder = Schema::builder();
//             schema_builder.add_u64_field("row_id", FAST | INDEXED);
//             schema_builder.add_text_field("text", TEXT | STORED);
//             let schema = schema_builder.build();
//             // Create the index in the specified directory.
//             let index = Index::create_in_dir(index_directory_str.to_string(), schema.clone())
//                 .expect("Can't create index");
//             // Create the writer with a specified buffer size (e.g., 64 MB).
//             let mut writer = index
//                 .writer_with_num_threads(2, 1024 * 1024 * 64)
//                 .expect("Can't create index writer");
//             // Configure default merge policy.
//             writer.set_merge_policy(Box::new(LogMergePolicy::default()));
//             // Index some docs.
//             let docs: Vec<String> = vec![
//                 "Ancient empires rise and fall, shaping history's course.".to_string(),
//                 "Artistic expressions reflect diverse cultural heritages.".to_string(),
//                 "Social movements transform societies, forging new paths.".to_string(),
//                 "Strategic military campaigns alter the balance of power.".to_string(),
//                 "Ancient philosophies provide wisdom for modern dilemmas.".to_string(),
//             ];
//             for row_id in 0..docs.len() {
//                 let mut doc = Document::default();
//                 doc.add_u64(schema.get_field("row_id").unwrap(), row_id as u64);
//                 doc.add_text(schema.get_field("text").unwrap(), &docs[row_id]);
//                 assert!(writer.add_document(doc).is_ok());
//             }
//             assert!(writer.commit().is_ok());

//             let reader = index
//                 .reader_builder()
//                 .reload_policy(ReloadPolicy::OnCommit)
//                 .try_into()
//                 .expect("Can't set reload policy");

//             IndexReaderBridge {
//                 path: index_directory_str.to_string(),
//                 index: index.clone(),
//                 reader: reader.clone(),
//             }
//         }

//         #[test]
//         fn test_perform_search_with_range() {
//             let temp_path = TempDir::new().unwrap();
//             let temp_path_str = temp_path.path().to_str().unwrap();
//             let index_reader_bridge = index_some_docs_in_temp_directory(temp_path_str);
//             let result = FFiIndexSearcherUtils::perform_search_with_range(
//                 &index_reader_bridge,
//                 "ancient",
//                 0,
//                 1,
//                 false,
//             )
//             .unwrap();
//             assert_eq!(result.len(), 1);
//         }

//         #[test]
//         fn test_intersect_and_return_normal_case() {
//             // Create a RoaringBitmap (1~10)
//             let mut bitmap = RoaringBitmap::new();
//             for i in 1..=10 {
//                 bitmap.insert(i);
//             }
//             let arc_bitmap = Arc::new(bitmap);
//             // target bitmap is contained by given bitmap
//             let result =
//                 FFiIndexSearcherUtils::intersect_and_return(arc_bitmap.clone(), 3, 7).unwrap();
//             let expected: RoaringBitmap = (3..=7).collect();
//             assert_eq!(*result, expected);
//         }

//         #[test]
//         fn test_intersect_and_return_boundary_1() {
//             let mut bitmap = RoaringBitmap::new();
//             bitmap.insert(5);
//             let arc_bitmap = Arc::new(bitmap);
//             // target bitmap has one value too.
//             let result =
//                 FFiIndexSearcherUtils::intersect_and_return(arc_bitmap.clone(), 5, 5).unwrap();
//             let expected: RoaringBitmap = [5].iter().cloned().collect();
//             assert_eq!(*result, expected);

//             // no intersetion
//             let result =
//                 FFiIndexSearcherUtils::intersect_and_return(arc_bitmap.clone(), 1, 4).unwrap();
//             let expected: RoaringBitmap = RoaringBitmap::new();
//             assert_eq!(*result, expected);
//         }

//         #[test]
//         fn test_intersect_and_return_boundary_2() {
//             let bitmap = RoaringBitmap::new();
//             let arc_bitmap = Arc::new(bitmap);

//             let result =
//                 FFiIndexSearcherUtils::intersect_and_return(arc_bitmap.clone(), u64::MAX, 10);
//             assert!(result.is_err());
//             assert_eq!(
//                 result.unwrap_err(),
//                 "lrange value is too large and causes u32 overflow"
//             );

//             let result =
//                 FFiIndexSearcherUtils::intersect_and_return(arc_bitmap.clone(), 1, u64::MAX);
//             assert!(result.is_err());
//             assert_eq!(
//                 result.unwrap_err(),
//                 "rrange value is too large and causes u32 overflow"
//             );
//         }
//     } }

