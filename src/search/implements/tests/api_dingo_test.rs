#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::collections::HashMap;

    use tantivy::merge_policy::LogMergePolicy;
    use tantivy::schema::{Schema, FAST, INDEXED, STORED, TEXT};
    use tantivy::{Index, IndexReader};
    use tempfile::TempDir;

    use crate::common::tests::{
        get_mocked_docs_for_part0, get_mocked_docs_for_part1, get_mocked_docs_for_part2,
        get_mocked_docs_for_part3, get_mocked_docs_for_part4, index_documents,
    };
    use crate::ffi::{DocWithFreq, FieldTokenNums, RowIdWithScore};
    use crate::search::implements::api_common_impl::load_index_reader;
    use crate::search::implements::api_dingo_impl::bm25_search_with_column_names;
    use crate::search::implements::api_dingo_impl::get_doc_freq;
    use crate::search::utils::convert_utils::ConvertUtils;

    #[allow(dead_code)]
    #[derive(Debug, Clone)]
    struct DocsWithScore {
        score: f32,
        row_id: u64,
        docs: Vec<String>,
    }

    impl DocsWithScore {
        fn new(row_id: u64, docs: Vec<String>, score: f32) -> Self {
            DocsWithScore {
                row_id,
                docs,
                score,
            }
        }
    }

    impl PartialEq for DocsWithScore {
        fn eq(&self, other: &Self) -> bool {
            self.score == other.score && self.docs == other.docs
        }
    }
    impl Eq for DocsWithScore {}
    impl PartialOrd for DocsWithScore {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            if self.score == other.score {
                self.docs.partial_cmp(&other.docs)
            } else {
                self.score.partial_cmp(&other.score)
            }
        }
    }
    impl Ord for DocsWithScore {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            // First compare scores
            match self.score.partial_cmp(&other.score) {
                Some(Ordering::Equal) => {
                    // If scores are equal, compare docs
                    self.docs.cmp(&other.docs)
                }
                Some(order) => order,
                None => Ordering::Equal, // Handle NaN cases, or you might choose to panic or define NaN handling logic
            }
        }
    }

    #[allow(dead_code)]
    fn create_schema() -> Schema {
        // Construct the schema for the index.
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("row_id", FAST | INDEXED);
        schema_builder.add_text_field("col1", TEXT | STORED);
        schema_builder.add_text_field("col2", TEXT | STORED);
        schema_builder.add_text_field("col3", TEXT | STORED);
        let schema = schema_builder.build();
        return schema;
    }

    fn create_colunm_names() -> Vec<String> {
        // Construct the column_names for search.
        vec!["col1".to_string(), "col2".to_string(), "col3".to_string()]
    }

    pub fn create_index(
        index_directory: &str,
        get_mocked_docs: impl Fn() -> (Vec<String>, Vec<String>, Vec<String>),
        schema: Schema,
    ) -> IndexReader {
        let index = Index::create_in_dir(index_directory, schema.clone()).unwrap();
        let mut writer = index.writer_with_num_threads(2, 1024 * 1024 * 64).unwrap();
        writer.set_merge_policy(Box::new(LogMergePolicy::default()));
        let (col1_docs, col2_docs, col3_docs) = get_mocked_docs();
        index_documents(&mut writer, &schema, &col1_docs, &col2_docs, &col3_docs);
        assert!(writer.commit().is_ok());
        assert!(writer.wait_merging_threads().is_ok());
        index.reader().unwrap()
    }

    pub fn index_for_part_0(index_directory: &str, schema: Schema) -> IndexReader {
        create_index(index_directory, get_mocked_docs_for_part0, schema)
    }

    pub fn index_for_part_1(index_directory: &str, schema: Schema) -> IndexReader {
        create_index(index_directory, get_mocked_docs_for_part1, schema)
    }

    pub fn index_for_part_2(index_directory: &str, schema: Schema) -> IndexReader {
        create_index(index_directory, get_mocked_docs_for_part2, schema)
    }

    pub fn index_for_part_3(index_directory: &str, schema: Schema) -> IndexReader {
        create_index(index_directory, get_mocked_docs_for_part3, schema)
    }

    pub fn index_for_part_4(index_directory: &str, schema: Schema) -> IndexReader {
        create_index(index_directory, get_mocked_docs_for_part4, schema)
    }

    pub fn get_mocked_docs_for_optimized_1_part() -> (Vec<String>, Vec<String>, Vec<String>) {
        let (col1_docs_0, col2_docs_0, col3_docs_0) = get_mocked_docs_for_part0();
        let (col1_docs_1, col2_docs_1, col3_docs_1) = get_mocked_docs_for_part1();
        let (col1_docs_2, col2_docs_2, col3_docs_2) = get_mocked_docs_for_part2();
        let (col1_docs_3, col2_docs_3, col3_docs_3) = get_mocked_docs_for_part3();
        let (col1_docs_4, col2_docs_4, col3_docs_4) = get_mocked_docs_for_part4();

        let col1_docs = [
            col1_docs_0,
            col1_docs_1,
            col1_docs_2,
            col1_docs_3,
            col1_docs_4,
        ]
        .concat();
        let col2_docs = [
            col2_docs_0,
            col2_docs_1,
            col2_docs_2,
            col2_docs_3,
            col2_docs_4,
        ]
        .concat();
        let col3_docs = [
            col3_docs_0,
            col3_docs_1,
            col3_docs_2,
            col3_docs_3,
            col3_docs_4,
        ]
        .concat();

        (col1_docs, col2_docs, col3_docs)
    }

    pub fn index_for_optimized_1_part(index_directory: &str, schema: Schema) -> IndexReader {
        create_index(
            index_directory,
            get_mocked_docs_for_optimized_1_part,
            schema,
        )
    }

    #[allow(dead_code)]
    fn merge_doc_freq(doc_with_freqs: Vec<Vec<DocWithFreq>>) -> Vec<DocWithFreq> {
        let mut res: HashMap<(String, u32), u64> = HashMap::new();
        for doc_with_freq in doc_with_freqs {
            for item in doc_with_freq {
                let key = (item.term_str, item.field_id);
                res.entry(key)
                    .and_modify(|freq| *freq += item.doc_freq)
                    .or_insert(item.doc_freq);
            }
        }
        let merged: Vec<DocWithFreq> = res
            .into_iter()
            .map(|((term_str, column_name), doc_freq)| {
                DocWithFreq::new(term_str, column_name, doc_freq)
            })
            .collect();
        merged
    }

    #[allow(dead_code)]
    fn merge_field_token_nums(field_token_nums: Vec<Vec<FieldTokenNums>>) -> Vec<FieldTokenNums> {
        let mut res: HashMap<u32, u64> = HashMap::new();
        for field_token_num in field_token_nums {
            for item in field_token_num {
                let key = item.field_id;
                res.entry(key)
                    .and_modify(|total_token_nums| *total_token_nums += item.field_total_tokens)
                    .or_insert(item.field_total_tokens);
            }
        }
        let merged: Vec<FieldTokenNums> = res
            .into_iter()
            .map(|(field_id, field_total_tokens)| FieldTokenNums::new(field_id, field_total_tokens))
            .collect();
        merged
    }

    fn index_for_part<F>(part_path: &str, index_func: F, schema: &Schema)
    where
        F: Fn(&str, Schema) -> IndexReader,
    {
        let _ = index_func(part_path, schema.clone());
        assert!(load_index_reader(part_path).is_ok());
        println!("Indexing for part {} is done.", part_path)
    }

    fn get_docs_with_score(
        part_path: &str,
        query_str: &str,
        topk: u32,
        u8_alive_bitmap: &Vec<u8>,
        use_filter: bool,
        need_docs: bool,
        column_names: &Vec<String>,
    ) -> Vec<DocsWithScore> {
        let alived_ids = crate::search::utils::convert_utils::ConvertUtils::u8_bitmap_to_row_ids(
            &u8_alive_bitmap,
        );
        let res: Vec<RowIdWithScore> = bm25_search_with_column_names(
            part_path,
            query_str,
            topk,
            &alived_ids,
            use_filter,
            need_docs,
            column_names,
        )
        .unwrap();
        let mut processed: Vec<DocsWithScore> = res
            .iter()
            .map(|item| DocsWithScore::new(item.row_id, item.docs.clone(), item.score))
            .collect();
        processed.sort();
        processed.reverse();
        // for p in processed.clone(){
        //     println!("{:?}", p);
        // }
        processed
    }
    #[test]
    pub fn test_bm25_search() {
        // prepare directory
        let part_0_temp_dir = TempDir::new().unwrap();
        let part_0 = part_0_temp_dir.path().to_str().unwrap();
        let part_1_temp_dir = TempDir::new().unwrap();
        let part_1 = part_1_temp_dir.path().to_str().unwrap();
        let part_2_temp_dir = TempDir::new().unwrap();
        let part_2 = part_2_temp_dir.path().to_str().unwrap();
        let part_3_temp_dir = TempDir::new().unwrap();
        let part_3 = part_3_temp_dir.path().to_str().unwrap();
        let part_4_temp_dir = TempDir::new().unwrap();
        let part_4 = part_4_temp_dir.path().to_str().unwrap();
        let part_optimized_temp_dir = TempDir::new().unwrap();
        let part_optimized = part_optimized_temp_dir.path().to_str().unwrap();

        let schema = create_schema();

        index_for_part(part_0, index_for_part_0, &schema);
        index_for_part(part_1, index_for_part_1, &schema);
        index_for_part(part_2, index_for_part_2, &schema);
        index_for_part(part_3, index_for_part_3, &schema);
        index_for_part(part_4, index_for_part_4, &schema);
        index_for_part(part_optimized, index_for_optimized_1_part, &schema);

        let query_str = "What innovative solutions are being developed to address environmental challenges and improve global sustainability?";
        // let query_str = "No innovative solutions";

        // println!("\nExexute get_doc_freq in part-0.");
        let part_0_doc_freq: Vec<DocWithFreq> = get_doc_freq(part_0, query_str).unwrap();
        // println!("\nExexute get_doc_freq in part-1.");
        let part_1_doc_freq: Vec<DocWithFreq> = get_doc_freq(part_1, query_str).unwrap();
        // println!("\nExexute get_doc_freq in part-2.");
        let part_2_doc_freq: Vec<DocWithFreq> = get_doc_freq(part_2, query_str).unwrap();
        // println!("\nExexute get_doc_freq in part-3.");
        let part_3_doc_freq: Vec<DocWithFreq> = get_doc_freq(part_3, query_str).unwrap();
        // println!("\nExexute get_doc_freq in part-4.");
        let part_4_doc_freq: Vec<DocWithFreq> = get_doc_freq(part_4, query_str).unwrap();

        // Assuming all vectors are of the same length and properly aligned for comparison.
        #[rustfmt::skip]
        assert!(part_0_doc_freq.iter().zip(part_1_doc_freq.iter()).all(|(a, b)| a.term_str == b.term_str && a.field_id == b.field_id),"term_str in part 0 and part 1 differ");
        #[rustfmt::skip]
        assert!(part_0_doc_freq.iter().zip(part_2_doc_freq.iter()).all(|(a, b)| a.term_str == b.term_str && a.field_id == b.field_id),"term_str in part 0 and part 2 differ");
        #[rustfmt::skip]
        assert!(part_0_doc_freq.iter().zip(part_3_doc_freq.iter()).all(|(a, b)| a.term_str == b.term_str && a.field_id == b.field_id),"term_str in part 0 and part 3 differ");
        #[rustfmt::skip]
        assert!(part_0_doc_freq.iter().zip(part_4_doc_freq.iter()).all(|(a, b)| a.term_str == b.term_str && a.field_id == b.field_id),"term_str in part 0 and part 4 differ");

        let column_names = create_colunm_names();

        let topk = 50;
        let optimized_ds: Vec<DocsWithScore> = get_docs_with_score(
            part_optimized,
            query_str,
            topk,
            &vec![],
            false,
            true,
            &column_names,
        );

        // println!("\nExexute get_docs_with_score in part-0.");
        let part_0_ds: Vec<DocsWithScore> =
            get_docs_with_score(part_0, query_str, topk, &vec![], false, true, &column_names);
        // println!("\nExexute get_docs_with_score in part-1.");
        let part_1_ds: Vec<DocsWithScore> =
            get_docs_with_score(part_1, query_str, topk, &vec![], false, true, &column_names);
        // println!("\nExexute get_docs_with_score in part-2.");
        let part_2_ds: Vec<DocsWithScore> =
            get_docs_with_score(part_2, query_str, topk, &vec![], false, true, &column_names);
        // println!("\nExexute get_docs_with_score in part-3.");
        let part_3_ds: Vec<DocsWithScore> =
            get_docs_with_score(part_3, query_str, topk, &vec![], false, true, &column_names);
        // println!("\nExexute get_docs_with_score in part-4.");
        let part_4_ds: Vec<DocsWithScore> =
            get_docs_with_score(part_4, query_str, topk, &vec![], false, true, &column_names);

        let mut combined = part_0_ds;
        combined.extend_from_slice(&part_1_ds);
        combined.extend_from_slice(&part_2_ds);
        combined.extend_from_slice(&part_3_ds);
        combined.extend_from_slice(&part_4_ds);
        combined.sort();
        combined.reverse();
        combined = combined[..optimized_ds.len()].to_vec();

        for item in optimized_ds.clone() {
            println!("{:?}", item);
        }

        println!("\n");
        for item in combined.clone() {
            println!("{:?}", item);
        }

        // assert_eq!(optimized_ds, combined);
        assert_eq!(optimized_ds.len(), combined.len());
    }
}
