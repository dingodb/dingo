use std::collections::BinaryHeap;
use std::sync::Arc;
use std::{cmp, fmt};

use roaring::RoaringBitmap;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::query::Weight;
use tantivy::schema::Field;
use tantivy::{DocAddress, DocId, Score, Searcher, SegmentOrdinal, SegmentReader};

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
// @`text_field` is an Option type from which the `searcher` reads the original text stored in the index.
// @`need_text` indicates whether the original text needs to be read from the index. If this is true, but either `searcher` or `text_field` is None, the original text will not be retrieved.

static INITIAL_HEAP_SIZE: usize = 1000;

pub struct TopDocsWithFilter {
    pub limit: usize,
    pub row_id_bitmap: Option<Arc<RoaringBitmap>>,
    pub searcher: Option<Searcher>,
    pub text_field: Option<Field>,
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
            text_field: None,
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
    pub fn with_text_field(mut self, field: Field) -> TopDocsWithFilter {
        self.text_field = Some(field.clone());
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
    fn extract_doc_text(&self, doc: DocId, segment_ord: SegmentOrdinal) -> String {
        let mut doc_text = String::new();
        if self.need_text {
            if let Some(searcher) = &self.searcher {
                if let Ok(document) = searcher.doc(DocAddress {
                    segment_ord,
                    doc_id: doc,
                }) {
                    if let Some(text_field) = self.text_field {
                        if let Some(field_value) = document.get_first(text_field) {
                            if let Some(text_value) = field_value.as_text() {
                                doc_text = text_value.to_string();
                            }
                        }
                    }
                }
            }
        }
        doc_text
    }
}

impl fmt::Debug for TopDocsWithFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TopDocsWithFilter(limit:{}, row_ids_size:{}, text_field_is_some:{}, searcher_is_some:{}, need_text:{}, initial_heap_size:{})",
            self.limit,
            if self.row_id_bitmap.is_some() {self.row_id_bitmap.clone().unwrap().len()} else {0},
            self.text_field.is_some(),
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
                    && !self
                        .row_id_bitmap
                        .clone()
                        .unwrap()
                        .contains(row_id as u32)
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
                    doc: self.extract_doc_text(doc, segment_ord),
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
                    doc: self.extract_doc_text(doc, segment_ord),
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

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::merge_policy::LogMergePolicy;
    use tantivy::query::{Query, QueryParser};
    use tantivy::schema::{Schema, FAST, INDEXED, STORED, TEXT};
    use tantivy::{Document, Index, IndexReader, IndexWriter, ReloadPolicy, Term};
    use tempfile::TempDir;

    fn get_reader_and_writer_from_index_path(index_directory_str: &str) -> (IndexReader, IndexWriter) {
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

        (reader, writer)
    }

    fn extract_from_index_reader(
        index_reader: IndexReader,
    ) -> (Field, QueryParser, Box<dyn Query>, Searcher) {
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
        (
            text_field,
            query_parser,
            text_query,
            index_reader.searcher(),
        )
    }

    #[test]
    fn test_normal_search() {
        let temp_path = TempDir::new().expect("Can't create temp path");
        let temp_path_str = temp_path.path().to_str().unwrap();
        let (index_reader, _) = get_reader_and_writer_from_index_path(temp_path_str);

        // Prepare variables for search.
        let (text_field, _, text_query, index_searcher) =
            extract_from_index_reader(index_reader.clone());

        let mut top_docs_collector = TopDocsWithFilter::with_limit(10)
            .with_searcher(index_searcher.clone())
            .with_text_field(text_field)
            .with_stored_text(false);

        let mut alive_bitmap = RoaringBitmap::new();
        alive_bitmap.extend(0..3);

        // Not use alive bitmap
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 2);

        // Use alive bitmap
        top_docs_collector = top_docs_collector.with_alive(Arc::new(alive_bitmap));
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 1);
    }

    #[test]
    fn test_search_after_delete() {
        let temp_path = TempDir::new().expect("Can't create temp path");
        let temp_path_str = temp_path.path().to_str().unwrap();
        let (index_reader, mut index_writer) = get_reader_and_writer_from_index_path(temp_path_str);

        // Delete term row_id=0.
        let term: Term = Term::from_field_u64(index_writer.index().schema().get_field("row_id").unwrap(), 0);
        let _ = index_writer.delete_term(term);
        assert!(index_writer.commit().is_ok());

        // Prepare variables for search.
        assert!(index_reader.reload().is_ok());
        let (text_field, _, text_query, index_searcher) =
            extract_from_index_reader(index_reader.clone());

        let top_docs_collector = TopDocsWithFilter::with_limit(10)
            .with_searcher(index_searcher.clone())
            .with_text_field(text_field)
            .with_stored_text(false);

        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 1);
    }

    #[test]
    fn test_boundary_searcher() {
        let temp_path = TempDir::new().expect("Can't create temp path");
        let temp_path_str = temp_path.path().to_str().unwrap();

        // Prepare variables for search.
        let (index_reader, _) = get_reader_and_writer_from_index_path(temp_path_str);
        let (text_field, _, text_query, index_searcher) =
            extract_from_index_reader(index_reader.clone());

        // The Searcher is designed to retrieve documents stored in the index.
        // Use searcher.
        let top_docs_collector = TopDocsWithFilter::with_limit(10)
            .with_searcher(index_searcher.clone())
            .with_text_field(text_field)
            .with_stored_text(true);
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 2);
        assert_eq!(
            searched_results[0].doc,
            "Ancient philosophies provide wisdom for modern dilemmas."
        );
        assert_eq!(
            searched_results[1].doc,
            "Ancient empires rise and fall, shaping history's course."
        );

        // Not use searcher.
        let top_docs_collector = TopDocsWithFilter::with_limit(10)
            .with_text_field(text_field)
            .with_stored_text(true);
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 2);
        assert_eq!(searched_results[0].doc, "");
        assert_eq!(searched_results[1].doc, "");
    }

    #[test]
    fn test_boundary_stored_text() {
        let temp_path = TempDir::new().expect("Can't create temp path");
        let temp_path_str = temp_path.path().to_str().unwrap();

        // Prepare variables for search.
        let (index_reader, _) = get_reader_and_writer_from_index_path(temp_path_str);
        let (text_field, _, text_query, index_searcher) =
            extract_from_index_reader(index_reader.clone());

        // The field `stored_text` can dicide whether to retrieve documents stored in the index.
        // Need stored text.
        let top_docs_collector = TopDocsWithFilter::with_limit(10)
            .with_searcher(index_searcher.clone())
            .with_text_field(text_field)
            .with_stored_text(true);
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 2);
        assert_eq!(
            searched_results[0].doc,
            "Ancient philosophies provide wisdom for modern dilemmas."
        );
        assert_eq!(
            searched_results[1].doc,
            "Ancient empires rise and fall, shaping history's course."
        );

        // Not need stored text.
        let top_docs_collector = TopDocsWithFilter::with_limit(10)
            .with_searcher(index_searcher.clone())
            .with_text_field(text_field)
            .with_stored_text(false);
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 2);
        assert_eq!(searched_results[0].doc, "");
        assert_eq!(searched_results[1].doc, "");
    }

    #[test]
    fn test_boundary_text_field() {
        let temp_path = TempDir::new().expect("Can't create temp path");
        let temp_path_str = temp_path.path().to_str().unwrap();

        // Prepare variables for search.
        let (index_reader, _) = get_reader_and_writer_from_index_path(temp_path_str);
        let (text_field, _, text_query, index_searcher) =
            extract_from_index_reader(index_reader.clone());

        // If need to retrieve stored doc, we will find it under field `text_field`, if field `text_filed` is none, we won't load stored doc.
        // With `text_field`.
        let top_docs_collector = TopDocsWithFilter::with_limit(10)
            .with_searcher(index_searcher.clone())
            .with_text_field(text_field)
            .with_stored_text(true);
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 2);
        assert_eq!(
            searched_results[0].doc,
            "Ancient philosophies provide wisdom for modern dilemmas."
        );
        assert_eq!(
            searched_results[1].doc,
            "Ancient empires rise and fall, shaping history's course."
        );

        // Without `text_field`
        let top_docs_collector = TopDocsWithFilter::with_limit(10)
            .with_searcher(index_searcher.clone())
            .with_stored_text(true);
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 2);
        assert_eq!(searched_results[0].doc, "");
        assert_eq!(searched_results[1].doc, "");
    }

    #[test]
    fn test_boundary_limit() {
        let temp_path = TempDir::new().expect("Can't create temp path");
        let temp_path_str = temp_path.path().to_str().unwrap();

        // Prepare variables for search.
        let (index_reader, _) = get_reader_and_writer_from_index_path(temp_path_str);
        let (text_field, _, text_query, index_searcher) =
            extract_from_index_reader(index_reader.clone());

        // The field `limit` can decide how many results will be returned.
        // limit size = 3.
        let top_docs_collector = TopDocsWithFilter::with_limit(3)
            .with_searcher(index_searcher.clone())
            .with_text_field(text_field)
            .with_stored_text(true);
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 2);
        assert_eq!(
            searched_results[0].doc,
            "Ancient philosophies provide wisdom for modern dilemmas."
        );
        assert_eq!(
            searched_results[1].doc,
            "Ancient empires rise and fall, shaping history's course."
        );
        // limit size = 1.
        let top_docs_collector = TopDocsWithFilter::with_limit(1)
            .with_searcher(index_searcher.clone())
            .with_text_field(text_field)
            .with_stored_text(true);
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 1);
        assert_eq!(
            searched_results[0].doc,
            "Ancient philosophies provide wisdom for modern dilemmas."
        );

        // limit size = 0.
        let top_docs_collector = TopDocsWithFilter::with_limit(0)
            .with_searcher(index_searcher.clone())
            .with_text_field(text_field)
            .with_stored_text(false);
        let searched_results = index_searcher
            .search(&text_query, &top_docs_collector)
            .expect("Can't execute search.");
        assert_eq!(searched_results.len(), 0);
    }
}
