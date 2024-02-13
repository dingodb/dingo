use std::sync::Arc;

use roaring::RoaringBitmap;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::columnar::Column;
use tantivy::{Score, SegmentReader};

/*
    Struct visualization.

    +-----------------------+       +------------------------------+
    |                       |       |                              |
    | RowIdRoaringCollector |       | RowIdRoaringSegmentCollector |
    |                       |       |                              |
    +-----------------------+       +------------------------------+
          | implements                    | implements
          |                               |
          v                               v
    +------------+ [Trait]           +------------------+ [Trait]
    |            |                   |                  |
    |  Collector + <-.-.-.-.-.-.-.-. + SegmentCollector |
    |            |     Child Type    |                  |
    +------------+                   +------------------+
*/

pub struct RowIdRoaringCollector {
    pub row_id_field: String,
}

impl RowIdRoaringCollector {
    pub fn with_field(row_id_field: String) -> RowIdRoaringCollector {
        RowIdRoaringCollector { row_id_field }
    }
}

impl Collector for RowIdRoaringCollector {
    type Fruit = Arc<RoaringBitmap>;
    type Child = RowIdRoaringSegmentCollector;

    // Create `RowIdRoaringSegmentCollector` for each segment.
    fn for_segment(
        &self,
        _segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let row_id_reader_ = segment_reader.fast_fields().u64(&self.row_id_field)?;
        Ok(RowIdRoaringSegmentCollector::new(row_id_reader_))
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    // Merge fruits thorough each segment.
    fn merge_fruits(
        &self,
        segment_row_ids: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut row_id_roaring_bitmap = RoaringBitmap::new();
        for segment_row_id_roaring_bitmap in segment_row_ids {
            match Arc::try_unwrap(segment_row_id_roaring_bitmap) {
                Ok(bitmap) => row_id_roaring_bitmap |= bitmap,
                Err(arc_bitmap) => {
                    // for multi reference, need call clone()
                    row_id_roaring_bitmap |= arc_bitmap.as_ref().clone();
                }
            }
        }
        Ok(Arc::new(row_id_roaring_bitmap))
    }
}

pub struct RowIdRoaringSegmentCollector {
    row_id_reader: Column,
    row_id_roaring_bitmap: Arc<RoaringBitmap>,
}

impl RowIdRoaringSegmentCollector {
    pub fn new(row_id_reader: Column) -> Self {
        RowIdRoaringSegmentCollector {
            row_id_reader,
            row_id_roaring_bitmap: Arc::new(RoaringBitmap::new()),
        }
    }
}

impl SegmentCollector for RowIdRoaringSegmentCollector {
    type Fruit = Arc<RoaringBitmap>;

    fn collect(&mut self, doc: u32, _score: Score) {
        let row_ids: Vec<u32> = self
            .row_id_reader
            .values_for_doc(doc)
            .filter_map(|row_id| {
                if row_id <= u32::MAX as u64 {
                    Some(row_id as u32)
                } else {
                    None
                }
            })
            .collect();

        match Arc::get_mut(&mut self.row_id_roaring_bitmap) {
            Some(bitmap) => bitmap.extend(row_ids),
            None => println!("Failed to get mutable reference to RoaringBitmap"),
        }
    }

    fn harvest(self) -> <Self as SegmentCollector>::Fruit {
        // Arc::clone(&self.row_id_roaring_bitmap)
        self.row_id_roaring_bitmap
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::collector::Collector;
    use tantivy::merge_policy::LogMergePolicy;
    use tantivy::query::QueryParser;
    use tantivy::schema::{Schema, FAST, INDEXED, STORED, TEXT};
    use tantivy::{Document, Index, IndexReader, ReloadPolicy};
    use tempfile::TempDir;

    fn get_reader_from_index_path(index_directory_str: &str) -> IndexReader {
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
            let mut doc = Document::default();
            doc.add_u64(schema.get_field("row_id").unwrap(), row_id as u64);
            doc.add_text(schema.get_field("text").unwrap(), &docs[row_id]);
            assert!(writer.add_document(doc).is_ok());
        }
        assert!(writer.commit().is_ok());

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()
            .expect("Can't set reload policy");

        reader
    }

    #[test]
    fn test_rowid_roaring_collector_with_field() {
        let collector = RowIdRoaringCollector::with_field("row_id".to_string());
        assert_eq!(collector.row_id_field, "row_id");
    }

    #[test]
    fn test_rowid_roaring_collector_merge_fruits() {
        let collector = RowIdRoaringCollector::with_field("row_id".to_string());
        let bitmap1 = Arc::new(RoaringBitmap::from_iter(vec![1, 2, 3]));
        let bitmap2 = Arc::new(RoaringBitmap::from_iter(vec![3, 4, 5, 6]));
        let merged = collector.merge_fruits(vec![bitmap1, bitmap2]).unwrap();
        let expected_bitmap: RoaringBitmap = (1..=6).collect();
        assert_eq!(merged, expected_bitmap.into());
    }

    #[test]
    fn test_rowid_roaring_segment_collector() {
        let temp_path = TempDir::new().expect("Can't create temp path");
        let temp_path_str = temp_path.path().to_str().unwrap();
        let index_reader = get_reader_from_index_path(temp_path_str);

        let text_field = index_reader
            .searcher()
            .index()
            .schema()
            .get_field("text")
            .unwrap();

        let query_parser =
            QueryParser::for_index(&index_reader.searcher().index(), vec![text_field]);
        let text_query = query_parser
            .parse_query("Ancient")
            .expect("Can't parse query");
        let row_id_collector = RowIdRoaringCollector::with_field("row_id".to_string());

        let searched_bitmap_1 = index_reader
            .searcher()
            .search(&text_query, &row_id_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_bitmap_1.len(), 2);
    }
}
