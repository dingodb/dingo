use crate::logger::logger_bridge::TantivySearchLogger;
use crate::{common::constants::LOG_CALLBACK, INFO};
use tantivy::{Index, IndexReader};

pub struct IndexReaderBridge {
    pub path: String,
    pub index: Index,
    pub reader: IndexReader,
}

impl Drop for IndexReaderBridge {
    fn drop(&mut self) {
        INFO!(
            "IndexReaderBridge has been dropped. index_path:[{}]",
            self.path
        );
    }
}

impl IndexReaderBridge {
    #[allow(dead_code)]
    pub fn reader_address(&self) -> usize {
        &self.reader as *const IndexReader as usize
    }
    pub fn reload(&self) -> Result<(), String> {
        self.reader.reload().map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use tantivy::{
        merge_policy::LogMergePolicy,
        query::QueryParser,
        schema::{Schema, FAST, INDEXED, STORED, TEXT},
        Index, IndexWriter, ReloadPolicy, TantivyDocument, Term,
    };
    use tempfile::TempDir;

    use crate::search::{
        bridge::index_reader_bridge::IndexReaderBridge,
        collector::row_id_bitmap_collector::RowIdRoaringCollector,
    };

    fn index_some_docs_in_temp_directory(
        index_directory_str: &str,
    ) -> (IndexReaderBridge, IndexWriter) {
        // Construct the schema for the index.
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("row_id", FAST | INDEXED);
        schema_builder.add_text_field("text", TEXT | STORED);
        let schema = schema_builder.build();
        // Create the index in the specified directory.
        let index = Index::create_in_dir(index_directory_str.to_string(), schema.clone())
            .expect("Can't create index");
        // Create the writer with a specified buffer size (e.g., 64 MB).
        let mut writer = index
            .writer_with_num_threads(2, 1024 * 1024 * 64)
            .expect("Can't create index writer");
        // Configure default merge policy.
        writer.set_merge_policy(Box::new(LogMergePolicy::default()));
        // Index some docs.
        let docs: Vec<String> = vec![
            "Ancient empires rise and fall, shaping history's course.".to_string(),
            "Artistic expressions reflect diverse cultural heritages.".to_string(),
            "Social movements transform societies, forging new paths.".to_string(),
            "Strategic military campaigns alter the balance of power.".to_string(),
            "Ancient philosophies provide wisdom for modern dilemmas.".to_string(),
        ];
        for row_id in 0..docs.len() {
            let mut doc = TantivyDocument::default();
            doc.add_u64(schema.get_field("row_id").unwrap(), row_id as u64);
            doc.add_text(schema.get_field("text").unwrap(), &docs[row_id]);
            assert!(writer.add_document(doc).is_ok());
        }
        assert!(writer.commit().is_ok());

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .expect("Can't set reload policy");

        (
            IndexReaderBridge {
                path: index_directory_str.to_string(),
                index: index.clone(),
                reader: reader.clone(),
            },
            writer,
        )
    }

    #[test]
    fn test_get_reader_address() {
        let temp_directory_1 = TempDir::new().expect("Can't create temp directory");
        let temp_directory_2 = TempDir::new().expect("Can't create temp directory");
        let temp_directory_str_1 = temp_directory_1
            .path()
            .to_str()
            .expect("Can't get temp directory str");
        let temp_directory_str_2 = temp_directory_2
            .path()
            .to_str()
            .expect("Can't get temp directory str");
        let (index_reader_bridge_1, _) = index_some_docs_in_temp_directory(temp_directory_str_1);
        let (index_reader_bridge_2, _) = index_some_docs_in_temp_directory(temp_directory_str_2);
        assert_ne!(
            index_reader_bridge_1.reader_address(),
            index_reader_bridge_2.reader_address()
        );
    }

    #[test]
    fn test_reload_reader() {
        let temp_directory = TempDir::new().expect("Can't create temp directory");
        let temp_directory_str = temp_directory
            .path()
            .to_str()
            .expect("Can't get temp directory str");
        let (index_reader_bridge, mut index_writer) =
            index_some_docs_in_temp_directory(temp_directory_str);

        // Init some variables for search.
        let text_field = index_reader_bridge
            .index
            .schema()
            .get_field("text")
            .unwrap();
        let row_id_field = index_reader_bridge
            .index
            .schema()
            .get_field("row_id")
            .unwrap();
        let query_parser = QueryParser::for_index(&index_reader_bridge.index, vec![text_field]);
        let text_query = query_parser
            .parse_query("Ancient")
            .expect("Can't parse query");
        let row_id_collector = RowIdRoaringCollector::with_field("row_id".to_string());

        // Execute search before delete.
        let reader = index_reader_bridge
            .index
            .reader()
            .expect("Can't get reader from index");
        let searched_bitmap_1 = reader
            .searcher()
            .search(&text_query, &row_id_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_bitmap_1.len(), 2);

        // Delete row_id terms.
        let term = Term::from_field_u64(row_id_field, 0);
        index_writer.delete_term(term);
        assert!(index_writer.commit().is_ok());

        // Execute search after delete.
        let searched_bitmap_2 = reader
            .searcher()
            .search(&text_query, &row_id_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_bitmap_2.len(), 2);

        // Execute search after reader reload.
        assert!(reader.reload().is_ok());
        let searched_bitmap_3 = reader
            .searcher()
            .search(&text_query, &row_id_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_bitmap_3.len(), 1);
    }
}
