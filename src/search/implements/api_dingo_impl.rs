use crate::common::errors::TantivySearchError;
use crate::ffi::DocWithFreq;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::bridge::index_reader_bridge::IndexReaderBridge;
use crate::RowIdWithScore;
use crate::FFI_INDEX_SEARCHER_CACHE;
use crate::{common::constants::LOG_CALLBACK, ERROR, WARNING};
use std::sync::Arc;
use tantivy::query::Bm25StatisticsProvider;

use super::strategy::query_strategy::BM25QueryStrategy;
use super::strategy::query_strategy::QueryExecutor;
use crate::DEBUG;
use crate::TRACE;
use tantivy::schema::FieldType;
use tantivy::schema::Schema;
use tantivy::schema::TextFieldIndexing;
use tantivy::tokenizer::BoxTokenStream;
use tantivy::tokenizer::TextAnalyzer;
use tantivy::Term;

pub fn bm25_search_with_column_names(
    index_path: &str,
    sentence: &str,
    topk: u32,
    alived_ids: &Vec<u32>,
    query_with_filter: bool,
    need_doc: bool,
    column_names: &Vec<String>,
) -> Result<Vec<RowIdWithScore>, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"bm25_search", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    // Choose query strategy to construct query executor.
    let sentence_query: BM25QueryStrategy<'_> = BM25QueryStrategy {
        sentence,
        topk: &topk,
        alived_ids,
        query_with_filter: &query_with_filter,
        need_doc: &need_doc,
        column_names: &column_names,
    };

    let query_executor: QueryExecutor<'_, Vec<RowIdWithScore>> =
        QueryExecutor::new(&sentence_query);

    let searcher = &mut index_reader_bridge.reader.searcher();

    let result: Vec<RowIdWithScore> = query_executor.execute(searcher).map_err(
        |e: crate::common::errors::IndexSearcherError| {
            ERROR!(function:"bm25_search", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        },
    )?;

    Ok(result)
}

pub fn index_reader_reload(index_path: &str) -> Result<bool, TantivySearchError> {
    // Try reload index reader from CACHE
    let reload_status = match FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
    {
        Ok(current_index_reader) => match current_index_reader.reload() {
            Ok(_) => true,
            Err(e) => {
                ERROR!(function: "delete_row_ids", "Can't reload reader after delete operation: {}", e);
                return Err(TantivySearchError::InternalError(e));
            }
        },
        Err(e) => {
            WARNING!(function: "delete_row_ids", "{}, skip reload it. ", e);
            true
        }
    };

    Ok(reload_status)
}

pub fn get_doc_freq(
    index_path: &str,
    sentence: &str,
) -> Result<Vec<DocWithFreq>, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"get_total_num_docs", "{}", e);
            TantivySearchError::InternalError(e)
        })?;
    let searcher = index_reader_bridge.reader.searcher();

    let schema: Schema = index_reader_bridge.index.schema();
    let mut terms: Vec<Term> = Vec::new();

    for (col_field, col_field_entry) in schema.fields() {
        let field_type = col_field_entry.field_type();
        if !field_type.is_indexed() {
            continue;
        }
        if let FieldType::Str(ref str_options) = field_type {
            let indexing_options: &TextFieldIndexing =
                str_options.get_indexing_options().ok_or_else(|| {
                    let error_msg: String = format!(
                        "column field:{} not indexed, but this error msg shouldn't display",
                        col_field_entry.name()
                    );
                    ERROR!(function:"get_doc_freq", "{}", error_msg);
                    TantivySearchError::InternalError(error_msg)
                })?;
            let mut text_analyzer: TextAnalyzer = searcher
                .index()
                .tokenizers()
                .get(indexing_options.tokenizer())
                .unwrap();
            let mut token_stream: BoxTokenStream<'_> = text_analyzer.token_stream(sentence);
            token_stream.process(&mut |token| {
                let term: Term = Term::from_field_text(col_field, &token.text);
                terms.push(term);
            });
            // MyScale Only Support single column.
            break;
        }
    }
    let mut doc_with_freq_vector: Vec<DocWithFreq> = vec![];
    for term in terms {
        let doc_freq = searcher.doc_freq(&term).map_err(|e| {
            ERROR!(function:"get_doc_freq", "{}", e);
            TantivySearchError::TantivyError(e)
        })?;
        let doc_with_freq = DocWithFreq::new(
            term.value().as_str().unwrap_or("").to_string(),
            term.field().field_id(),
            doc_freq,
        );
        TRACE!(function:"get_doc_freq", "{:?}", doc_with_freq);
        doc_with_freq_vector.push(doc_with_freq);
    }
    Ok(doc_with_freq_vector)
}

pub fn get_total_num_docs(index_path: &str) -> Result<u64, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"get_total_num_docs", "{}", e);
            TantivySearchError::InternalError(e)
        })?;
    let searcher = index_reader_bridge.reader.searcher();
    let total_num_docs = searcher
        .total_num_docs()
        .map_err(|e| TantivySearchError::TantivyError(e))?;
    DEBUG!(function:"get_total_num_docs", "total_num_docs is {}", total_num_docs);
    Ok(total_num_docs)
}

pub fn get_total_num_tokens(index_path: &str) -> Result<u64, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"get_total_num_tokens", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    let schema: Schema = index_reader_bridge.index.schema();
    let searcher = index_reader_bridge.reader.searcher();

    for (col_field, col_field_entry) in schema.fields() {
        let field_type = col_field_entry.field_type();
        if !field_type.is_indexed() {
            continue;
        }
        if let FieldType::Str(_) = field_type {
            let field_total_num_tokens = searcher
                .total_num_tokens(col_field)
                .map_err(|e| TantivySearchError::TantivyError(e))?;

            DEBUG!(function:"get_total_num_tokens", "total_num_tokens for field-id:{} is {}", col_field.field_id(), field_total_num_tokens);
            // MyScale Only Support single column.
            return Ok(field_total_num_tokens);
        }
    }
    Ok(0u64)
}
