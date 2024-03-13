use std::{str::FromStr, sync::Arc};

use cang_jie::{CangJieTokenizer, TokenizerOption};
use jieba_rs::Jieba;
use tantivy::{
    tokenizer::{
        LowerCaser, NgramTokenizer, RawTokenizer, RemoveLongFilter, SimpleTokenizer, Stemmer,
        StopWordFilter, TextAnalyzer, WhitespaceTokenizer,
    },
    Index,
};

use crate::common::errors::TokenizerUtilsError;

use super::vo::{
    language::{SupportFilterLanguage, SupportLanguageAlgorithm},
    tokenizer_json_vo::ColumnTokenizer,
    tokenizer_types::TokenizerType,
    tokenizers_vo::TokenizerConfig,
};

pub struct ToeknizerUtils;

impl ToeknizerUtils {
    // Register the tokenizer to the index
    pub fn register_tokenizer_to_index(
        index: &mut Index,
        tokenizer_type: TokenizerType,
        column_name: &str,
        tokenizer: TextAnalyzer,
    ) -> Result<String, TokenizerUtilsError> {
        #[allow(unreachable_patterns)]
        match tokenizer_type {
            TokenizerType::Default(tokenizer_name) => {
                index
                    .tokenizers()
                    .register(&format!("{}_{}", column_name, tokenizer_name), tokenizer);
                Ok(format!(
                    "`{}-{}` tokenizer has been registered",
                    column_name, tokenizer_name
                ))
            }
            TokenizerType::Raw(tokenizer_name) => {
                index
                    .tokenizers()
                    .register(&format!("{}_{}", column_name, tokenizer_name), tokenizer);
                Ok(format!(
                    "`{}-{}` tokenizer has been registered",
                    column_name, tokenizer_name
                ))
            }
            TokenizerType::Simple(tokenizer_name) => {
                index
                    .tokenizers()
                    .register(&format!("{}_{}", column_name, tokenizer_name), tokenizer);
                Ok(format!(
                    "`{}-{}` tokenizer has been registered",
                    column_name, tokenizer_name
                ))
            }
            TokenizerType::Stem(tokenizer_name) => {
                index
                    .tokenizers()
                    .register(&format!("{}_{}", column_name, tokenizer_name), tokenizer);
                Ok(format!(
                    "`{}-{}` tokenizer has been registered",
                    column_name, tokenizer_name
                ))
            }
            TokenizerType::WhiteSpace(tokenizer_name) => {
                index
                    .tokenizers()
                    .register(&format!("{}_{}", column_name, tokenizer_name), tokenizer);
                Ok(format!(
                    "`{}-{}` tokenizer has been registered",
                    column_name, tokenizer_name
                ))
            }
            TokenizerType::Ngram(tokenizer_name) => {
                index
                    .tokenizers()
                    .register(&format!("{}_{}", column_name, tokenizer_name), tokenizer);
                Ok(format!(
                    "`{}-{}` tokenizer has been registered",
                    column_name, tokenizer_name
                ))
            }
            TokenizerType::Chinese(tokenizer_name) => {
                index
                    .tokenizers()
                    .register(&format!("{}_{}", column_name, tokenizer_name), tokenizer);
                Ok(format!(
                    "`{}-{}` tokenizer has been registered",
                    column_name, tokenizer_name
                ))
            }
            _ => Err(TokenizerUtilsError::UnsupportedTokenizerType(
                tokenizer_type.name().to_string(),
            )),
        }
    }

    pub fn parse_tokenizer_json_to_config_map(
        json_str: &str,
    ) -> Result<std::collections::HashMap<String, TokenizerConfig>, TokenizerUtilsError> {
        let config: crate::tokenizer::vo::tokenizer_json_vo::Config =
            serde_json::from_str(json_str)
                .map_err(|e| TokenizerUtilsError::JsonDeserializeError(e.to_string()))?;

        let mut tokenizer_map: std::collections::HashMap<String, TokenizerConfig> =
            std::collections::HashMap::new();

        for (col_name, col) in config.get_columns() {
            match col.get_tokenizer() {
                ColumnTokenizer::Default { store_doc } => {
                    let analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
                        .filter(RemoveLongFilter::limit(40))
                        .filter(LowerCaser)
                        .build();
                    let tokenizer_config = TokenizerConfig::new(
                        TokenizerType::Default("default".to_string()),
                        analyzer,
                        *store_doc,
                    );
                    tokenizer_map.insert(col_name.to_string(), tokenizer_config);
                }
                ColumnTokenizer::Raw { store_doc } => {
                    let analyzer = TextAnalyzer::builder(RawTokenizer::default()).build();
                    let tokenizer_config = TokenizerConfig::new(
                        TokenizerType::Raw("raw".to_string()),
                        analyzer,
                        *store_doc,
                    );
                    tokenizer_map.insert(col_name.to_string(), tokenizer_config);
                }
                ColumnTokenizer::Simple {
                    store_doc,
                    stop_word_filters,
                    length_limit,
                    case_sensitive,
                } => {
                    let mut builder = TextAnalyzer::builder(SimpleTokenizer::default()).dynamic();

                    builder = builder.filter_dynamic(RemoveLongFilter::limit(*length_limit));

                    for stop_word_filter in stop_word_filters {
                        let language = SupportFilterLanguage::from_str(stop_word_filter.as_str())
                            .unwrap()
                            .to_language();
                        if language.is_some() {
                            builder = builder
                                .filter_dynamic(StopWordFilter::new(language.unwrap()).unwrap());
                        }
                    }

                    if *case_sensitive == false {
                        builder = builder.filter_dynamic(LowerCaser);
                    }

                    let tokenizer_config = TokenizerConfig::new(
                        TokenizerType::Simple("simple".to_string()),
                        builder.build(),
                        *store_doc,
                    );
                    tokenizer_map.insert(col_name.to_string(), tokenizer_config);
                }
                ColumnTokenizer::Stem {
                    stop_word_filters,
                    stem_languages,
                    store_doc,
                    length_limit,
                    case_sensitive,
                } => {
                    let mut builder = TextAnalyzer::builder(SimpleTokenizer::default()).dynamic();

                    builder = builder.filter_dynamic(RemoveLongFilter::limit(*length_limit));

                    for stop_word_filter in stop_word_filters {
                        let language = SupportFilterLanguage::from_str(stop_word_filter.as_str())
                            .unwrap()
                            .to_language();
                        if language.is_some() {
                            builder = builder
                                .filter_dynamic(StopWordFilter::new(language.unwrap()).unwrap());
                        }
                    }

                    for stem_language in stem_languages {
                        let language = SupportLanguageAlgorithm::from_str(stem_language.as_str())
                            .unwrap()
                            .to_language();
                        if language.is_some() {
                            builder = builder.filter_dynamic(Stemmer::new(language.unwrap()));
                        }
                    }

                    if *case_sensitive == false {
                        builder = builder.filter_dynamic(LowerCaser);
                    }

                    let tokenizer_config = TokenizerConfig::new(
                        TokenizerType::Stem("stem".to_string()),
                        builder.build(),
                        *store_doc,
                    );
                    tokenizer_map.insert(col_name.to_string(), tokenizer_config);
                }
                ColumnTokenizer::Whitespace {
                    store_doc,
                    stop_word_filters,
                    length_limit,
                    case_sensitive,
                } => {
                    let mut builder =
                        TextAnalyzer::builder(WhitespaceTokenizer::default()).dynamic();

                    builder = builder.filter_dynamic(RemoveLongFilter::limit(*length_limit));

                    for stop_word_filter in stop_word_filters {
                        let language = SupportFilterLanguage::from_str(stop_word_filter.as_str())
                            .unwrap()
                            .to_language();
                        if language.is_some() {
                            builder = builder
                                .filter_dynamic(StopWordFilter::new(language.unwrap()).unwrap());
                        }
                    }

                    if *case_sensitive == false {
                        builder = builder.filter_dynamic(LowerCaser);
                    }

                    let tokenizer_config = TokenizerConfig::new(
                        TokenizerType::WhiteSpace("whitespace".to_string()),
                        builder.build(),
                        *store_doc,
                    );
                    tokenizer_map.insert(col_name.to_string(), tokenizer_config);
                }
                ColumnTokenizer::Ngram {
                    min_gram,
                    max_gram,
                    prefix_only,
                    store_doc,
                    stop_word_filters,
                    length_limit,
                    case_sensitive,
                } => {
                    if min_gram >= max_gram || (*min_gram == 0 && *max_gram == 0) {
                        return Err(TokenizerUtilsError::JsonParseError(
                            "`min_gram` should be smaller than `max_gram`".to_string(),
                        ));
                    }

                    let mut builder = TextAnalyzer::builder(
                        NgramTokenizer::new(*min_gram, *max_gram, *prefix_only).map_err(|e| {
                            TokenizerUtilsError::ConfigTokenizerError(e.to_string())
                        })?,
                    )
                    .dynamic();

                    builder = builder.filter_dynamic(RemoveLongFilter::limit(*length_limit));

                    for stop_word_filter in stop_word_filters {
                        let language = SupportFilterLanguage::from_str(stop_word_filter.as_str())
                            .unwrap()
                            .to_language();
                        if language.is_some() {
                            builder = builder
                                .filter_dynamic(StopWordFilter::new(language.unwrap()).unwrap());
                        }
                    }

                    if *case_sensitive == false {
                        builder = builder.filter_dynamic(LowerCaser);
                    }

                    let tokenizer_config = TokenizerConfig::new(
                        TokenizerType::Ngram("ngram".to_string()),
                        builder.build(),
                        *store_doc,
                    );
                    tokenizer_map.insert(col_name.to_string(), tokenizer_config);
                }
                ColumnTokenizer::Chinese {
                    jieba,
                    mode,
                    hnm,
                    store_doc,
                    length_limit,
                } => {
                    let jieba_mode: Jieba = match jieba.as_str() {
                        "default" => Jieba::default(),
                        "empty" => Jieba::empty(),
                        _ => Jieba::empty(),
                    };

                    let tokenizer_option: TokenizerOption = match mode.as_str() {
                        "all" => TokenizerOption::All,
                        "unicode" => TokenizerOption::Unicode,
                        "default" => TokenizerOption::Default { hmm: *hnm },
                        "search" => TokenizerOption::ForSearch { hmm: *hnm },
                        _ => TokenizerOption::Unicode, // default option
                    };

                    let mut builder = TextAnalyzer::builder(CangJieTokenizer {
                        worker: Arc::new(jieba_mode),
                        option: tokenizer_option,
                    })
                    .dynamic();
                    builder = builder.filter_dynamic(RemoveLongFilter::limit(*length_limit));

                    let tokenizer_config = TokenizerConfig::new(
                        TokenizerType::Chinese("chinese".to_string()),
                        builder.build(),
                        *store_doc,
                    );
                    tokenizer_map.insert(col_name.to_string(), tokenizer_config);
                }
            }
        }
        Ok(tokenizer_map)
    }

    pub fn varify_json_parameter(
        json_str: &str
    ) -> Result<bool, TokenizerUtilsError> {
        let _: crate::tokenizer::vo::tokenizer_json_vo::Config =
        serde_json::from_str(json_str)
            .map_err(|e| TokenizerUtilsError::JsonDeserializeError(e.to_string()))?;
        Ok(true)
    }

}
