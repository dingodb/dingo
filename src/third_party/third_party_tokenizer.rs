use std::sync::Arc;

use cang_jie::{CangJieTokenizer, TokenizerOption, CANG_JIE};
use jieba_rs::Jieba;
use tantivy::{schema::{IndexRecordOption, TextFieldIndexing, TextOptions}, Index};

pub enum ThirdPartyTokenizer {
    Chinese(CangJieTokenizer),
    // Japan(LinderaTokenizer),
}

// Get the third-party tokenizer
pub fn get_third_party_tokenizer(language: &str) -> (Option<String>, Option<ThirdPartyTokenizer>, TextOptions) {
    let language_lowercase = language.to_lowercase();
    let mut third_party_tokenizer: Option<ThirdPartyTokenizer> = None;
    let mut third_party_tokenizer_name: Option<String> = None;

    let third_party_text_options = match language_lowercase.as_str() {
        // Initialize Chinese tokenizer
        "chinese" => {
            third_party_tokenizer = Some(ThirdPartyTokenizer::Chinese(CangJieTokenizer {
                worker: Arc::new(Jieba::empty()),
                option: TokenizerOption::Unicode,
            }));
            third_party_tokenizer_name = Some(CANG_JIE.to_string());

            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(CANG_JIE)
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
        }
        // Handle the default tokenizer
        _ => TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("default")
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        ),
    };

    (third_party_tokenizer_name, third_party_tokenizer, third_party_text_options)
}

// Register the third-party tokenizer to the index
pub fn register_tokenizer_to_index(
    index: &mut Index, 
    third_party_tokenizer_name: Option<String>,
    third_party_tokenizer: Option<ThirdPartyTokenizer>
) -> Result<(), String> {
    // Attempt to register the tokenizer
    if let Some(tokenizer_name) = third_party_tokenizer_name.as_ref() {
        if let Some(tokenizer) = third_party_tokenizer {
            // Process the registration logic for different types of ThirdPartyTokenizer
            match tokenizer {
                ThirdPartyTokenizer::Chinese(chinese_tokenizer) => {
                    index.tokenizers().register(tokenizer_name, chinese_tokenizer);
                },
                // Add processing logic for other types of tokenizers
                // ThirdPartyTokenizer::Japanese(japanese_tokenizer) => { ... }
                // ...
            }
        } else {
            return Err("ThirdPartyTokenizer object is missing".to_string());
        }
    } else {
        return Err("ThirdPartyTokenizer name is missing".to_string());
    }
    Ok(())
}