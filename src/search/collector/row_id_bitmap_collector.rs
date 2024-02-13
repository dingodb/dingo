use std::sync::Arc;

use roaring::RoaringBitmap;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::columnar::Column;
use tantivy::{Score, SegmentReader};

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

    // each segment one SegmentCollector
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

    fn merge_fruits(
        &self,
        segment_row_ids:  Vec<<Self::Child as SegmentCollector>::Fruit>,
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
    use tantivy::schema::{Schema, Field};
    use tantivy::{Index, Document};
    use tantivy::collector::Collector;
    use std::path::Path;

    // 测试 RowIdRoaringCollector::with_field 方法
    #[test]
    fn test_with_field() {
        let collector = RowIdRoaringCollector::with_field("row_id".to_string());
        assert_eq!(collector.row_id_field, "row_id");
    }

    // 需要更多的测试来模拟 SegmentReader 和验证 for_segment 方法

    // 测试 merge_fruits 方法
    #[test]
    fn test_merge_fruits() {
        // let collector = RowIdRoaringCollector::with_field("row_id".to_string());
        // let bitmap1 = Arc::new(RoaringBitmap::from_iter(vec![1, 2, 3]));
        // let bitmap2 = Arc::new(RoaringBitmap::from_iter(vec![4, 5, 6]));
        // let merged = collector.merge_fruits(vec![bitmap1, bitmap2]).unwrap();
        // let expected_bitmap: RoaringBitmap = (1..=6).collect();
        // assert_eq!(*Arc::try_unwrap(merged).unwrap(), expected_bitmap);
    }

    // 测试 RowIdRoaringSegmentCollector::collect 方法
    // 这将需要模拟 row_id_reader 和验证收集的结果
}
