use tantivy::tokenizer::TextAnalyzer;

use super::tokenizer_types::TokenizerType;

pub struct TokenizerConfig {
    pub tokenizer_type: TokenizerType,
    pub text_analyzer: TextAnalyzer,
    pub doc_store: bool,
    pub doc_index: bool,
    pub is_text_field: bool,
}
impl TokenizerConfig {
    pub fn new(tokenizer_type: TokenizerType, analyzer: TextAnalyzer, stored: bool) -> Self {
        Self {
            tokenizer_type: tokenizer_type.clone(),
            text_analyzer: analyzer.clone(),
            doc_store: stored,
            doc_index: true,
            is_text_field: true,
        }
    }

    pub fn new_non_text(tokenizer_type: TokenizerType, stored: bool, indexed: bool) -> Self {
        Self {
            tokenizer_type: tokenizer_type.clone(),
            text_analyzer: TextAnalyzer::default(),
            doc_store: stored,
            doc_index: indexed,
            is_text_field: false,
        }
    }
}
