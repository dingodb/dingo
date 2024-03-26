use std::collections::BinaryHeap;
use std::sync::Arc;
use std::{cmp, fmt};

use roaring::RoaringBitmap;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::query::Weight;
use tantivy::schema::{Field, Value};
use tantivy::{DocAddress, DocId, Score, Searcher, SegmentOrdinal, SegmentReader, TantivyDocument};

use crate::RowIdWithScore;

// Class Inheritance Diagram:
//
//   +---------------------+            +--------------------------+
//   | TopDocsWithFilter   |<-----------| TopScoreSegmentCollector |
//   +---------------------+            +--------------------------+
//
// Variables in TopDocWithFilter:
// @`limit` restricts the number of search results.
// @`searcher` is an Option type used to read the original text stored in the index.
// @`text_fields` is an Option type from which the `searcher` reads the original text stored in the index.
// @`need_text` indicates whether the original text needs to be read from the index. If this is true, but either `searcher` or `text_fields` is None, the original text will not be retrieved.

static INITIAL_HEAP_SIZE: usize = 1000;

pub struct TopDocsWithFilter {
    pub limit: usize,
    pub row_id_bitmap: Option<Arc<RoaringBitmap>>,
    pub searcher: Option<Searcher>,
    pub text_fields: Option<Vec<Field>>,
    pub need_text: bool,
    pub initial_heap_size: usize,
}

impl TopDocsWithFilter {
    // limit for result size.
    pub fn with_limit(limit: usize) -> TopDocsWithFilter {
        // assert!(limit >= 1, "Limit must be strictly greater than 0.");
        Self {
            limit,
            row_id_bitmap: None,
            searcher: None,
            text_fields: None,
            need_text: false,
            initial_heap_size: INITIAL_HEAP_SIZE,
        }
    }

    // `row_id_bitmap` is used to mark aive row_ids.
    pub fn with_alive(mut self, row_id_bitmap: Arc<RoaringBitmap>) -> TopDocsWithFilter {
        self.row_id_bitmap = Some(Arc::clone(&row_id_bitmap));
        self
    }

    // `searcher` is used to search origin text content.
    pub fn with_searcher(mut self, searcher: Searcher) -> TopDocsWithFilter {
        self.searcher = Some(searcher.clone());
        self
    }

    // field which store origin text content.
    pub fn with_text_fields(mut self, fields: Vec<Field>) -> TopDocsWithFilter {
        self.text_fields = Some(fields.clone());
        self
    }

    // whether need return origin text content.
    pub fn with_stored_text(mut self, need_text: bool) -> TopDocsWithFilter {
        self.need_text = need_text;
        self
    }

    // initial size for binary_heap
    #[allow(dead_code)]
    pub fn with_initial_heap_size(mut self, initial_heap_size: usize) -> TopDocsWithFilter {
        self.initial_heap_size = initial_heap_size;
        self
    }

    pub fn merge_fruits(
        &self,
        children: Vec<Vec<RowIdWithScore>>,
    ) -> tantivy::Result<Vec<RowIdWithScore>> {
        if self.limit == 0 {
            return Ok(Vec::new());
        }
        let mut top_collector = BinaryHeap::new();
        for child_fruit in children {
            for child in child_fruit {
                if top_collector.len() < self.limit {
                    top_collector.push(child);
                } else if let Some(mut head) = top_collector.peek_mut() {
                    if head.score < child.score {
                        *head = child;
                    }
                }
            }
        }
        Ok(top_collector.into_sorted_vec())
    }

    #[inline]
    fn extract_doc_text(&self, doc: DocId, segment_ord: SegmentOrdinal) -> Vec<String> {
        let mut doc_texts: Vec<String> = vec![];
        if self.need_text {
            if let Some(searcher) = &self.searcher {
                if let Ok(document) = searcher.doc::<TantivyDocument>(DocAddress {
                    segment_ord,
                    doc_id: doc,
                }) {
                    if let Some(text_fields) = &self.text_fields {
                        for text_field in text_fields {
                            if let Some(field_value) = document.get_first(*text_field) {
                                if let Some(text_value) = field_value.as_str() {
                                    doc_texts.push(text_value.to_string());
                                } else {
                                    doc_texts.push("".to_string())
                                }
                            }
                        }
                    }
                }
            }
        }
        doc_texts
    }
}

impl fmt::Debug for TopDocsWithFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TopDocsWithFilter(limit:{}, row_ids_size:{}, text_fields_is_some:{}, searcher_is_some:{}, need_text:{}, initial_heap_size:{})",
            self.limit,
            if self.row_id_bitmap.is_some() {self.row_id_bitmap.clone().unwrap().len()} else {0},
            self.text_fields.is_some(),
            self.searcher.is_some(),
            self.need_text,
            self.initial_heap_size
        )
    }
}

impl Collector for TopDocsWithFilter {
    type Fruit = Vec<RowIdWithScore>;

    type Child = TopScoreSegmentCollector; // won't use for current design.

    // won't use for current design.
    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        _reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(TopScoreSegmentCollector())
    }

    // won't use for current design.
    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, child_fruits: Vec<Vec<RowIdWithScore>>) -> tantivy::Result<Self::Fruit> {
        self.merge_fruits(child_fruits)
    }

    // collector for each segment.
    fn collect_segment(
        &self,
        weight: &dyn Weight,
        segment_ord: SegmentOrdinal,
        reader: &SegmentReader,
    ) -> tantivy::Result<<Self::Child as SegmentCollector>::Fruit> {
        // REFINE: need a more efficient way to initialize binary-heap.
        let heap_len = cmp::min(self.limit, self.initial_heap_size);
        let mut heap: BinaryHeap<RowIdWithScore> = BinaryHeap::with_capacity(heap_len);

        let row_id_field_reader = reader
            .fast_fields()
            .u64("row_id")
            .unwrap()
            .first_or_default_col(0);

        if let Some(alive_bitset) = reader.alive_bitset() {
            let mut threshold = Score::MIN;
            weight.for_each_pruning(threshold, reader, &mut |doc, score| {
                let row_id = row_id_field_reader.get_val(doc);
                if self.row_id_bitmap.is_some()
                    && !self.row_id_bitmap.clone().unwrap().contains(row_id as u32)
                {
                    return threshold;
                }
                if alive_bitset.is_deleted(doc) {
                    return threshold;
                }
                let heap_item = RowIdWithScore {
                    row_id,
                    score,
                    seg_id: segment_ord,
                    doc_id: doc,
                    docs: self.extract_doc_text(doc, segment_ord),
                };
                if heap.len() < heap_len {
                    heap.push(heap_item);
                    if heap.len() == heap_len {
                        threshold = heap.peek().map(|el| el.score).unwrap_or(Score::MIN);
                    }
                    return threshold;
                }
                *heap.peek_mut().unwrap() = heap_item;
                threshold = heap.peek().map(|el| el.score).unwrap_or(Score::MIN);
                threshold
            })?;
        } else {
            weight.for_each_pruning(Score::MIN, reader, &mut |doc, score| {
                let row_id = row_id_field_reader.get_val(doc);
                if self.row_id_bitmap.is_some()
                    && !self.row_id_bitmap.clone().unwrap().contains(row_id as u32)
                {
                    return Score::MIN;
                }
                let heap_item = RowIdWithScore {
                    row_id,
                    score,
                    seg_id: segment_ord,
                    doc_id: doc,
                    docs: self.extract_doc_text(doc, segment_ord),
                };
                if heap.len() < heap_len {
                    heap.push(heap_item);
                    // REFINE: the threshold is suboptimal for heap.len == heap_len
                    if heap.len() == heap_len {
                        return heap.peek().map(|el| el.score).unwrap_or(Score::MIN);
                    }
                    return Score::MIN;
                }
                if let Some(mut head) = heap.peek_mut() {
                    *head = heap_item;
                } else {
                    // limit size may be equal with zero.
                }
                heap.peek().map(|el| el.score).unwrap_or(Score::MIN)
            })?;
        }
        Ok(heap.into_sorted_vec())
    }
}

pub struct TopScoreSegmentCollector();

impl SegmentCollector for TopScoreSegmentCollector {
    type Fruit = Vec<RowIdWithScore>;

    fn collect(&mut self, _doc: DocId, _score: Score) {
        println!("Not implement");
    }

    fn harvest(self) -> Vec<RowIdWithScore> {
        println!("Not implement");
        vec![]
    }
}
