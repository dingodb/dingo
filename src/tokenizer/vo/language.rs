use std::str::FromStr;

use serde::{Deserialize, Serialize};
use tantivy::tokenizer::Language;

/// Available languages for stop words.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Copy, Clone)]
pub enum SupportFilterLanguage {
    Danish,
    Dutch,
    English,
    Finnish,
    French,
    German,
    Hungarian,
    Italian,
    Norwegian,
    Portuguese,
    Russian,
    Spanish,
    Swedish,
}

impl FromStr for SupportFilterLanguage {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "danish" => Ok(SupportFilterLanguage::Danish),
            "dutch" => Ok(SupportFilterLanguage::Dutch),
            "english" => Ok(SupportFilterLanguage::English),
            "finnish" => Ok(SupportFilterLanguage::Finnish),
            "french" => Ok(SupportFilterLanguage::French),
            "german" => Ok(SupportFilterLanguage::German),
            "hungarian" => Ok(SupportFilterLanguage::Hungarian),
            "italian" => Ok(SupportFilterLanguage::Italian),
            "norwegian" => Ok(SupportFilterLanguage::Norwegian),
            "portuguese" => Ok(SupportFilterLanguage::Portuguese),
            "russian" => Ok(SupportFilterLanguage::Russian),
            "spanish" => Ok(SupportFilterLanguage::Spanish),
            "swedish" => Ok(SupportFilterLanguage::Swedish),
            _ => Err(format!("Unknown filter language: {}", s)),
        }
    }
}

impl SupportFilterLanguage {
    #[allow(unreachable_patterns)]
    pub fn to_language(&self) -> Option<Language> {
        match self {
            SupportFilterLanguage::Danish => Some(Language::Danish),
            SupportFilterLanguage::Dutch => Some(Language::Dutch),
            SupportFilterLanguage::English => Some(Language::English),
            SupportFilterLanguage::Finnish => Some(Language::Finnish),
            SupportFilterLanguage::French => Some(Language::French),
            SupportFilterLanguage::German => Some(Language::German),
            SupportFilterLanguage::Hungarian => Some(Language::Hungarian),
            SupportFilterLanguage::Italian => Some(Language::Italian),
            SupportFilterLanguage::Norwegian => Some(Language::Norwegian),
            SupportFilterLanguage::Portuguese => Some(Language::Portuguese),
            SupportFilterLanguage::Russian => Some(Language::Russian),
            SupportFilterLanguage::Spanish => Some(Language::Spanish),
            SupportFilterLanguage::Swedish => Some(Language::Swedish),
            _ => None,
        }
    }
}

/// Available languages for stem algorithm.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Copy, Clone)]
pub enum SupportLanguageAlgorithm {
    Arabic,
    Danish,
    Dutch,
    English,
    Finnish,
    French,
    German,
    Greek,
    Hungarian,
    Italian,
    Norwegian,
    Portuguese,
    Romanian,
    Russian,
    Spanish,
    Swedish,
    Tamil,
    Turkish,
}

impl FromStr for SupportLanguageAlgorithm {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "arabic" => Ok(SupportLanguageAlgorithm::Arabic),
            "danish" => Ok(SupportLanguageAlgorithm::Danish),
            "dutch" => Ok(SupportLanguageAlgorithm::Dutch),
            "english" => Ok(SupportLanguageAlgorithm::English),
            "finnish" => Ok(SupportLanguageAlgorithm::Finnish),
            "french" => Ok(SupportLanguageAlgorithm::French),
            "german" => Ok(SupportLanguageAlgorithm::German),
            "greek" => Ok(SupportLanguageAlgorithm::Greek),
            "hungarian" => Ok(SupportLanguageAlgorithm::Hungarian),
            "italian" => Ok(SupportLanguageAlgorithm::Italian),
            "norwegian" => Ok(SupportLanguageAlgorithm::Norwegian),
            "portuguese" => Ok(SupportLanguageAlgorithm::Portuguese),
            "romanian" => Ok(SupportLanguageAlgorithm::Romanian),
            "russian" => Ok(SupportLanguageAlgorithm::Russian),
            "spanish" => Ok(SupportLanguageAlgorithm::Spanish),
            "swedish" => Ok(SupportLanguageAlgorithm::Swedish),
            "tamil" => Ok(SupportLanguageAlgorithm::Tamil),
            "turkish" => Ok(SupportLanguageAlgorithm::Turkish),
            _ => Err(format!("Unsupported language algorithm: {}", s)),
        }
    }
}

impl SupportLanguageAlgorithm {
    #[allow(unreachable_patterns)]
    pub fn to_language(&self) -> Option<Language> {
        match self {
            SupportLanguageAlgorithm::Arabic => Some(Language::Arabic),
            SupportLanguageAlgorithm::Danish => Some(Language::Danish),
            SupportLanguageAlgorithm::Dutch => Some(Language::Dutch),
            SupportLanguageAlgorithm::English => Some(Language::English),
            SupportLanguageAlgorithm::Finnish => Some(Language::Finnish),
            SupportLanguageAlgorithm::French => Some(Language::French),
            SupportLanguageAlgorithm::German => Some(Language::German),
            SupportLanguageAlgorithm::Greek => Some(Language::Greek),
            SupportLanguageAlgorithm::Hungarian => Some(Language::Hungarian),
            SupportLanguageAlgorithm::Italian => Some(Language::Italian),
            SupportLanguageAlgorithm::Norwegian => Some(Language::Norwegian),
            SupportLanguageAlgorithm::Portuguese => Some(Language::Portuguese),
            SupportLanguageAlgorithm::Romanian => Some(Language::Romanian),
            SupportLanguageAlgorithm::Russian => Some(Language::Russian),
            SupportLanguageAlgorithm::Spanish => Some(Language::Spanish),
            SupportLanguageAlgorithm::Swedish => Some(Language::Swedish),
            SupportLanguageAlgorithm::Tamil => Some(Language::Tamil),
            SupportLanguageAlgorithm::Turkish => Some(Language::Turkish),
            _ => None,
        }
    }
}
