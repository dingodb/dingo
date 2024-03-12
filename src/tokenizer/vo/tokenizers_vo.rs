use tantivy::tokenizer::TextAnalyzer;

use super::tokenizer_types::TokenizerType;

pub struct TokenizerConfig {
    pub tokenizer_type: TokenizerType,
    pub text_analyzer: TextAnalyzer,
    pub doc_store: bool,
}
impl TokenizerConfig {
    pub fn new(tokenizer_type: TokenizerType, analyzer: TextAnalyzer, stored: bool) -> Self {
        Self {
            tokenizer_type: tokenizer_type.clone(),
            text_analyzer: analyzer.clone(),
            doc_store: stored,
        }
    }
}
