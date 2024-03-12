use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize};

use super::language::{SupportFilterLanguage, SupportLanguageAlgorithm};

/// ColumnTokenizer stores the specific configuration information
/// for the tokenizer of each column. During the development process,
/// developers need to pay attention to handling the edge cases for each tokenizer type.
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type")]
pub enum ColumnTokenizer {
    #[serde(rename = "default")]
    Default {
        #[serde(default)]
        store_doc: bool,
    },
    #[serde(rename = "raw")]
    Raw {
        #[serde(default)]
        store_doc: bool,
    },
    #[serde(rename = "simple")]
    Simple {
        #[serde(default, deserialize_with = "stop_word_filters_validator")]
        stop_word_filters: Vec<String>,
        #[serde(default)]
        store_doc: bool,
        #[serde(default = "default_length_limit")]
        length_limit: usize,
        #[serde(default)]
        case_sensitive: bool,
    },
    #[serde(rename = "stem")]
    Stem {
        #[serde(default, deserialize_with = "stop_word_filters_validator")]
        stop_word_filters: Vec<String>,
        #[serde(default, deserialize_with = "stem_languages_filters_validator")]
        stem_languages: Vec<String>,
        #[serde(default)]
        store_doc: bool,
        #[serde(default = "default_length_limit")]
        length_limit: usize,
        #[serde(default)]
        case_sensitive: bool,
    },
    #[serde(rename = "whitespace")]
    Whitespace {
        #[serde(default, deserialize_with = "stop_word_filters_validator")]
        stop_word_filters: Vec<String>,
        #[serde(default)]
        store_doc: bool,
        #[serde(default = "default_length_limit")]
        length_limit: usize,
        #[serde(default)]
        case_sensitive: bool,
    },
    #[serde(rename = "ngram")]
    Ngram {
        #[serde(default = "default_min_gram")]
        min_gram: usize,
        #[serde(default = "default_max_gram")]
        max_gram: usize,
        #[serde(default)]
        prefix_only: bool,
        #[serde(default, deserialize_with = "stop_word_filters_validator")]
        stop_word_filters: Vec<String>,
        #[serde(default)]
        store_doc: bool,
        #[serde(default = "default_length_limit")]
        length_limit: usize,
        #[serde(default)]
        case_sensitive: bool,
    },
    #[serde(rename = "chinese")]
    Chinese {
        #[serde(
            default = "chinese_jieba_default",
            deserialize_with = "chinese_jieba_validator"
        )]
        jieba: String,
        #[serde(
            default = "chinese_mode_default",
            deserialize_with = "chinese_mode_validator"
        )]
        mode: String,
        #[serde(default)]
        hnm: bool,
        #[serde(default)]
        store_doc: bool,
        #[serde(default = "default_length_limit")]
        length_limit: usize,
    },
}

fn default_length_limit() -> usize {
    40
}

fn default_min_gram() -> usize {
    2
}

fn default_max_gram() -> usize {
    3
}

fn chinese_jieba_default() -> String {
    "default".to_string()
}
fn chinese_mode_default() -> String {
    "search".to_string()
}

fn chinese_jieba_validator<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let jieba = String::deserialize(deserializer)?;
    if jieba != "default" && jieba != "empty" {
        return Err(serde::de::Error::custom(format!(
            "Invalid value for jieba: {}. Expected 'default' or 'empty'.",
            jieba
        )));
    }
    Ok(jieba)
}

fn chinese_mode_validator<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let mode = String::deserialize(deserializer)?;
    if mode != "all" && mode != "default" && mode != "search" && mode != "unicode" {
        return Err(serde::de::Error::custom(format!(
            "Invalid value for mode: {}. Expected 'all' or 'xxxxxxxxxxxxx'.",
            mode
        )));
    }
    Ok(mode)
}

fn stop_word_filters_validator<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let stop_word_filters: Vec<String> = Vec::deserialize(deserializer)?;
    stop_word_filters
        .iter()
        .try_for_each(|item| match SupportFilterLanguage::from_str(item) {
            Ok(_) => Ok(()),
            Err(err) => Err(serde::de::Error::custom(format!(
                "Invalid stop word filter: {}, error: {}",
                item, err
            ))),
        })?;
    Ok(stop_word_filters)
}

fn stem_languages_filters_validator<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let stem_languages: Vec<String> = Vec::deserialize(deserializer)?;
    stem_languages
        .iter()
        .try_for_each(|item| match SupportLanguageAlgorithm::from_str(item) {
            Ok(_) => Ok(()),
            Err(err) => Err(serde::de::Error::custom(format!(
                "Invalid stop word filter: {}, error: {}",
                item, err
            ))),
        })?;
    Ok(stem_languages)
}

/// Column represents the specific configurations for each column.
/// Currently, it only supports adding the tokenizer configuration for each column.
/// In the future, it can be further extended to include more configuration options.
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Column {
    tokenizer: ColumnTokenizer,
}

impl Column {
    pub fn get_tokenizer(&self) -> &ColumnTokenizer {
        &self.tokenizer
    }
}

/// Config represents the parameter configuration passed by ClickHouse when creating a Tantivy index.
/// Currently, ClickHouse only passes a single string to Tantivy,
/// and this string needs to conform to the JSON5 specification and be parsed into the Config struct.
#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    #[serde(flatten)]
    columns: std::collections::HashMap<String, Column>,
}

impl Config {
    pub fn get_columns(&self) -> &std::collections::HashMap<String, Column> {
        &self.columns
    }
}
