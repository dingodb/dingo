use std::sync::Arc;

use roaring::RoaringBitmap;
use tantivy::query::{Query, QueryParser, QueryParserError, RegexQuery, TermQuery, TermSetQuery};
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::{TantivyError, Term};
use tantivy::{schema::Schema, Searcher};

use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::collector::row_id_bitmap_collector::RowIdRoaringCollector;
use crate::search::collector::top_dos_with_bitmap_collector::TopDocsWithFilter;
use crate::search::utils::convert_utils::ConvertUtils;
use crate::INFO;
use crate::{common::errors::IndexSearcherError, ffi::RowIdWithScore, ERROR};
use crate::common::constants::LOG_CALLBACK;

pub trait QueryStrategy<T> {
    fn execute(&self, searcher: &Searcher) -> Result<T, IndexSearcherError>;
}

/// Execute query for a group of terms.
///
/// Params:
/// - `column_name`: Execute query in which column.
/// - `terms`: A group of terms.
///
pub struct TermSetQueryStrategy<'a> {
    pub column_name: &'a str,
    pub terms: &'a Vec<String>,
}

impl<'a> QueryStrategy<Arc<RoaringBitmap>> for TermSetQueryStrategy<'a> {
    fn execute(&self, searcher: &Searcher) -> Result<Arc<RoaringBitmap>, IndexSearcherError> {
        let schema: Schema = searcher.index().schema();

        let col_field: Field = schema.get_field(self.column_name).map_err(|e|{
            let error: IndexSearcherError = IndexSearcherError::TantivyError(e);
            ERROR!(function:"TermSetQueryStrategy", "{}", error);
            error
        })?;

        let terms: Vec<Term> = self.terms.iter().map(|term|{
            Term::from_field_text(col_field, &term)
        }).collect();

        let ter_set_query: TermSetQuery = TermSetQuery::new(terms);
        let row_id_collector: RowIdRoaringCollector = RowIdRoaringCollector::with_field("row_id".to_string());
        
        searcher.search(&ter_set_query, &row_id_collector).map_err(|e|{
            ERROR!(function:"TermSetQueryStrategy", "{}", e);
            IndexSearcherError::TantivyError(e)
        })
    }
}

/// Execute query for one term.
///
/// Params:
/// - `column_name`: Execute query in which column.
/// - `term`: Term need to be queried.
///
pub struct SingleTermQueryStrategy<'a> {
    pub column_name: &'a str,
    pub term: &'a str,
}

impl<'a> QueryStrategy<Arc<RoaringBitmap>> for SingleTermQueryStrategy<'a> {
    fn execute(&self, searcher: &Searcher) -> Result<Arc<RoaringBitmap>, IndexSearcherError> {
        let schema: Schema = searcher.index().schema();

        let col_field: Field = schema.get_field(self.column_name).map_err(|e|{
            let error: IndexSearcherError = IndexSearcherError::TantivyError(e);
            ERROR!(function:"SingleTermQueryStrategy", "{}", error);
            error
        })?;

        let term: Term = Term::from_field_text(col_field, self.term);
        INFO!(function:"SingleTermQueryStrategy", "query term is {}", self.term);

        let term_query: TermQuery = TermQuery::new(term, IndexRecordOption::WithFreqs);
        let row_id_collector: RowIdRoaringCollector = RowIdRoaringCollector::with_field("row_id".to_string());
        
        let e = searcher.search(&term_query, &row_id_collector).map_err(|e|{
            ERROR!(function:"SingleTermQueryStrategy", "{}", e);
            IndexSearcherError::TantivyError(e)
        });
        INFO!(function:"SingleTermQueryStrategy", "return bitmap size is {}", e.clone().ok().unwrap().len());
        e
    }
}

/// Execute regex query for a given pattern.
///
/// Params:
/// - `column_name`: Execute query in which column.
/// - `pattern`: Regex query will execute with given pattern str.
///
pub struct RegexQueryStrategy<'a> {
    pub column_name: &'a str,
    pub pattern: &'a str,
}

impl<'a> QueryStrategy<Arc<RoaringBitmap>> for RegexQueryStrategy<'a> {
    fn execute(&self, searcher: &Searcher) -> Result<Arc<RoaringBitmap>, IndexSearcherError> {
        let schema: Schema = searcher.index().schema();

        let col_field: Field = schema.get_field(self.column_name).map_err(|e|{
            let error: IndexSearcherError = IndexSearcherError::TantivyError(e);
            ERROR!(function:"RegexQueryStrategy", "{}", error);
            error
        })?;

        let row_id_collector: RowIdRoaringCollector = RowIdRoaringCollector::with_field("row_id".to_string());
        let regex_query: RegexQuery = RegexQuery::from_pattern(&ConvertUtils::like_to_regex(self.pattern), col_field).map_err(|e|{
            ERROR!(function:"RegexQueryStrategy", "Error when parse regex query:{}. {}", ConvertUtils::like_to_regex(self.pattern), e);
            IndexSearcherError::TantivyError(e)
        })?;

        searcher.search(&regex_query, &row_id_collector).map_err(|e|{
            ERROR!(function:"RegexQueryStrategy", "Error when execute regex query:{}. {}", ConvertUtils::like_to_regex(self.pattern), e);
            IndexSearcherError::TantivyError(e)
        })
    }
}

/// Execute query for a sentence.
/// This sentence may be written by natural language, or just simple terms.
/// 
/// Params:
/// - `column_name`: Execute query in which column.
/// - `sentence`: Sentence need to be parsed and query. 
///
pub struct ParserQueryStrategy<'a> {
    pub column_name: &'a str,
    pub sentence: &'a str,
}

impl<'a> QueryStrategy<Arc<RoaringBitmap>> for ParserQueryStrategy<'a>  {
    fn execute(&self, searcher: &Searcher) -> Result<Arc<RoaringBitmap>, IndexSearcherError> {
        let schema: Schema = searcher.index().schema();

        let col_field: Field = schema.get_field(self.column_name).map_err(|e|{
            let error: IndexSearcherError = IndexSearcherError::TantivyError(e);
            ERROR!(function:"SingleTermQueryStrategy", "{}", error);
            error
        })?;

        let row_id_collector: RowIdRoaringCollector = RowIdRoaringCollector::with_field("row_id".to_string());
        let query_parser: QueryParser = QueryParser::for_index(searcher.index(), [col_field].to_vec());

        let text_query = query_parser.parse_query(self.sentence).map_err(|e|{
            ERROR!(function:"ParserQueryStrategy", "Error when parse: {}. {}", self.sentence, e);
            IndexSearcherError::QueryParserError(e.to_string())
        })?;

        searcher.search(&text_query, &row_id_collector).map_err(|e|{
            ERROR!(function:"ParserQueryStrategy", "Error when execute: {}. {}", self.sentence, e);
            IndexSearcherError::TantivyError(e)
        })
    }
}

/// Execute query for a sentence and get bm25 score.
/// Query will be run in all schema fields but `row_id`.
/// This sentence may be written by natural language, or just simple terms.
/// If `query_with_filter` is true, when calculating bm25 score, only in `alive_row_ids` will be recorded.
/// 
/// Params:
/// - `sentence`: Sentence need to be parsed and query. 
/// - `topk`: max-heap build with topK
/// - `u8_aived_bitmap`: Represent row_ids who are alived.
/// - `query_with_filter`: Whether collect row_ids with `u8_alived_bitmap`
///
pub struct BM25QueryStrategy<'a> {
    pub sentence: &'a str,
    pub topk: &'a u32,
    pub u8_aived_bitmap: &'a Vec<u8>,
    pub query_with_filter: &'a bool,
}

impl<'a> QueryStrategy<Vec<RowIdWithScore>> for BM25QueryStrategy<'a> {
    fn execute(&self, searcher: &Searcher) -> Result<Vec<RowIdWithScore>, IndexSearcherError> {
        let schema: Schema = searcher.index().schema();

        let fields: Vec<Field> = schema.fields().filter(|(field,_)|{
            schema.get_field_name(*field)!="row_id"
        }).map(|(field, _)| field).collect();

        let mut top_docs_collector: TopDocsWithFilter = TopDocsWithFilter::with_limit(*self.topk as usize)
            .with_searcher(searcher.clone())
            .with_text_fields(fields.clone())
            .with_stored_text(false);    

        // If query_with_filter is false, we regards that don't use alive_bitmap.
        if *self.query_with_filter {
            let mut alive_bitmap: RoaringBitmap = RoaringBitmap::new();
            alive_bitmap.extend(ConvertUtils::u8_bitmap_to_row_ids(self.u8_aived_bitmap));
            top_docs_collector = top_docs_collector.with_alive(Arc::new(alive_bitmap));
        }

        let query_parser: QueryParser = QueryParser::for_index(searcher.index(), fields);
        let text_query: Box<dyn Query> = query_parser.parse_query(self.sentence).map_err(|e: QueryParserError|{
            ERROR!(function:"BM25QueryStrategy", "Error when parse: {}. {}", self.sentence, e);
            IndexSearcherError::QueryParserError(e.to_string())
        })?;

        searcher.search(&text_query, &top_docs_collector).map_err(|e: TantivyError|{
            ERROR!(function:"BM25QueryStrategy", "Error when execute: {}. {}", self.sentence, e);
            IndexSearcherError::TantivyError(e)
        })
    }
}



pub struct QueryExecutor<'a, T> {
    strategy: &'a dyn QueryStrategy<T>,
}

impl<'a, T> QueryExecutor<'a, T> {
    pub fn new(strategy: &'a dyn QueryStrategy<T>) -> Self {
        QueryExecutor {strategy}
    }
    pub fn execute(&self, searcher: &Searcher) -> Result<T, IndexSearcherError> {
        self.strategy.execute(searcher)
    }
}