#[derive(Clone)]
pub enum TokenizerType {
    Default(String),
    Raw(String),
    Simple(String),
    Stem(String),
    WhiteSpace(String),
    Ngram(String),
    Chinese(String),
}

impl TokenizerType {
    pub fn name(&self) -> &str {
        match self {
            TokenizerType::Default(name) => name,
            TokenizerType::Raw(name) => name,
            TokenizerType::Simple(name) => name,
            TokenizerType::Stem(name) => name,
            TokenizerType::WhiteSpace(name) => name,
            TokenizerType::Ngram(name) => name,
            TokenizerType::Chinese(name) => name,
        }
    }
}
