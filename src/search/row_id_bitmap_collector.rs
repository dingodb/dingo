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
        segment_row_ids: Vec<Arc<RoaringBitmap>>,
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
        Arc::clone(&self.row_id_roaring_bitmap)
    }
}
