use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;
use std::{collections::BinaryHeap, marker::PhantomData};

use roaring::RoaringBitmap;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::query::Weight;
use tantivy::{DocAddress, DocId, Score, SegmentOrdinal, SegmentReader};

// Class Inheritance Diagram:
//
// +------------------------+
// | TopCollectorWithFilter |
// +------------------------+
//             ^
//             |
//             |
//   +---------------------+
//   | TopDocsWithFilter   |
//   +---------------------+
//
// Explanation:
// - `TopDocsWithFilter` can be treated as a subclass of `TopCollectorWithFilter`.
//

// `ComparableDoc` is same with tantivy.
struct ComparableDoc<T, D> {
    pub feature: T,
    pub doc: D,
}

impl<T: PartialOrd, D: PartialOrd> PartialOrd for ComparableDoc<T, D> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: PartialOrd, D: PartialOrd> Ord for ComparableDoc<T, D> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed to make BinaryHeap work as a min-heap
        let by_feature = other
            .feature
            .partial_cmp(&self.feature)
            .unwrap_or(Ordering::Equal);

        let lazy_by_doc_address = || self.doc.partial_cmp(&other.doc).unwrap_or(Ordering::Equal);

        // In case of a tie on the feature, we sort by ascending
        // `DocAddress` in order to ensure a stable sorting of the
        // documents.
        by_feature.then_with(lazy_by_doc_address)
    }
}

impl<T: PartialOrd, D: PartialOrd> PartialEq for ComparableDoc<T, D> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T: PartialOrd, D: PartialOrd> Eq for ComparableDoc<T, D> {}

pub struct TopCollectorWithFilter<T> {
    pub limit: usize,
    pub row_id_bitmap: Arc<RoaringBitmap>,
    _marker: PhantomData<T>,
}

impl<T> TopCollectorWithFilter<T>
where
    T: PartialOrd + Clone,
{
    pub fn with_limit(limit: usize) -> TopCollectorWithFilter<T> {
        assert!(limit >= 1, "Limit must be strictly greater than 0.");
        Self {
            limit,
            row_id_bitmap: Arc::new(RoaringBitmap::new()),
            _marker: PhantomData,
        }
    }

    pub fn and_row_id_bitmap(
        mut self,
        row_id_bitmap: Arc<RoaringBitmap>,
    ) -> TopCollectorWithFilter<T> {
        self.row_id_bitmap = Arc::clone(&row_id_bitmap);
        self
    }

    pub fn merge_fruits(
        &self,
        children: Vec<Vec<(T, DocAddress)>>,
    ) -> tantivy::Result<Vec<(T, DocAddress)>> {
        if self.limit == 0 {
            return Ok(Vec::new());
        }
        let mut top_collector = BinaryHeap::new();
        for child_fruit in children {
            for (feature, doc) in child_fruit {
                if top_collector.len() < self.limit {
                    top_collector.push(ComparableDoc { feature, doc });
                } else if let Some(mut head) = top_collector.peek_mut() {
                    if head.feature < feature {
                        *head = ComparableDoc { feature, doc };
                    }
                }
            }
        }
        Ok(top_collector
            .into_sorted_vec()
            .into_iter()
            .map(|cdoc| (cdoc.feature, cdoc.doc))
            .collect())
    }

    pub fn for_segment<F: PartialOrd>(
        &self,
        segment_id: SegmentOrdinal,
        _: &SegmentReader,
    ) -> TopSegmentCollector<F> {
        TopSegmentCollector::new(segment_id, self.limit)
    }
}

pub struct TopSegmentCollector<T> {
    limit: usize,
    heap: BinaryHeap<ComparableDoc<T, DocId>>, // default max-heap, reverse by ComparableDoc Ord.
    segment_ord: SegmentOrdinal,
}

impl<T: PartialOrd> TopSegmentCollector<T> {
    fn new(segment_ord: SegmentOrdinal, limit: usize) -> TopSegmentCollector<T> {
        TopSegmentCollector {
            limit,
            heap: BinaryHeap::with_capacity(limit), // 是否需要优化
            segment_ord,
        }
    }
}

impl<T: PartialOrd + Clone> TopSegmentCollector<T> {
    pub fn harvest(self) -> Vec<(T, DocAddress)> {
        let segment_ord = self.segment_ord;
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|comparable_doc| {
                (
                    comparable_doc.feature,
                    DocAddress {
                        segment_ord,
                        doc_id: comparable_doc.doc,
                    },
                )
            })
            .collect()
    }

    /// Return true if more documents have been collected than the limit.
    #[inline]
    pub(crate) fn at_capacity(&self) -> bool {
        self.heap.len() >= self.limit
    }

    /// Collects a document scored by the given feature
    ///
    /// It collects documents until it has reached the max capacity. Once it reaches capacity, it
    /// will compare the lowest scoring item with the given one and keep whichever is greater.
    #[inline]
    pub fn collect(&mut self, doc: DocId, feature: T) {
        if self.at_capacity() {
            // It's ok to unwrap as long as a limit of 0 is forbidden.
            if let Some(limit_feature) = self.heap.peek().map(|head| head.feature.clone()) {
                if limit_feature < feature {
                    if let Some(mut head) = self.heap.peek_mut() {
                        head.feature = feature;
                        head.doc = doc;
                    }
                }
            }
        } else {
            // we have not reached capacity yet, so we can just push the
            // element.
            self.heap.push(ComparableDoc { feature, doc });
        }
    }
}

// Score is an alias name for `f32`
pub struct TopDocsWithFilter(TopCollectorWithFilter<Score>); // tuple struct

impl fmt::Debug for TopDocsWithFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TopDocs(limit={}, row_id_bitmap_size={})",
            self.0.limit,
            self.0.row_id_bitmap.len()
        )
    }
}

impl TopDocsWithFilter {
    pub fn with_limit(limit: usize) -> TopDocsWithFilter {
        TopDocsWithFilter(TopCollectorWithFilter::with_limit(limit))
    }

    pub fn and_row_id_bitmap(self, row_id_bitmap: Arc<RoaringBitmap>) -> TopDocsWithFilter {
        TopDocsWithFilter(self.0.and_row_id_bitmap(row_id_bitmap))
    }
}

impl Collector for TopDocsWithFilter {
    type Fruit = Vec<(Score, DocAddress)>;

    type Child = TopScoreSegmentCollector;

    fn for_segment(
        &self,
        segment_local_id: SegmentOrdinal,
        reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let collector = self.0.for_segment(segment_local_id, reader);
        Ok(TopScoreSegmentCollector(collector))
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(
        &self,
        child_fruits: Vec<Vec<(Score, DocAddress)>>,
    ) -> tantivy::Result<Self::Fruit> {
        self.0.merge_fruits(child_fruits)
    }

    fn collect_segment(
        &self,
        weight: &dyn Weight,
        segment_ord: SegmentOrdinal,
        reader: &SegmentReader,
    ) -> tantivy::Result<<Self::Child as SegmentCollector>::Fruit> {
        let heap_len = self.0.limit;
        let mut heap: BinaryHeap<ComparableDoc<Score, DocId>> = BinaryHeap::with_capacity(heap_len);

        let row_id_field_reader = reader
            .fast_fields()
            .u64("row_id")
            .unwrap()
            .first_or_default_col(0);

        if let Some(alive_bitset) = reader.alive_bitset() {
            let mut threshold = Score::MIN;
            weight.for_each_pruning(threshold, reader, &mut |doc, score| {
                let row_id = row_id_field_reader.get_val(doc);
                if self.0.row_id_bitmap.contains(row_id as u32) {
                    return threshold;
                }
                if alive_bitset.is_deleted(doc) {
                    return threshold;
                }
                let heap_item = ComparableDoc {
                    feature: score,
                    doc,
                };
                if heap.len() < heap_len {
                    heap.push(heap_item);
                    if heap.len() == heap_len {
                        threshold = heap.peek().map(|el| el.feature).unwrap_or(Score::MIN);
                    }
                    return threshold;
                }
                *heap.peek_mut().unwrap() = heap_item;
                threshold = heap.peek().map(|el| el.feature).unwrap_or(Score::MIN);
                threshold
            })?;
        } else {
            weight.for_each_pruning(Score::MIN, reader, &mut |doc, score| {
                let row_id = row_id_field_reader.get_val(doc);
                if self.0.row_id_bitmap.contains(row_id as u32) {
                    return Score::MIN;
                }
                let heap_item = ComparableDoc {
                    feature: score,
                    doc,
                };
                if heap.len() < heap_len {
                    heap.push(heap_item);
                    // TODO the threshold is suboptimal for heap.len == heap_len
                    if heap.len() == heap_len {
                        return heap.peek().map(|el| el.feature).unwrap_or(Score::MIN);
                    } else {
                        return Score::MIN;
                    }
                }
                *heap.peek_mut().unwrap() = heap_item;
                heap.peek().map(|el| el.feature).unwrap_or(Score::MIN)
            })?;
        }

        let fruit = heap
            .into_sorted_vec()
            .into_iter()
            .map(|cid| {
                (
                    cid.feature,
                    DocAddress {
                        segment_ord,
                        doc_id: cid.doc,
                    },
                )
            })
            .collect();
        Ok(fruit)
    }
}

/// Segment Collector associated with `TopDocsWithFilter`.
pub struct TopScoreSegmentCollector(TopSegmentCollector<Score>);

impl SegmentCollector for TopScoreSegmentCollector {
    type Fruit = Vec<(Score, DocAddress)>;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.0.collect(doc, score);
    }

    fn harvest(self) -> Vec<(Score, DocAddress)> {
        self.0.harvest()
    }
}
