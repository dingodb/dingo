use serde::{Deserialize, Serialize};

/// `IndexParameterDTO` is used to record some custom configuration information about the index,
/// such as the tokenizer and tokenizer parameters.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct IndexParameterDTO {
    #[serde(default = "empty_json_parameter")]
    pub tokenizers_json_parameter: String,
}

impl Default for IndexParameterDTO {
    fn default() -> Self {
        Self {
            tokenizers_json_parameter: "{}".to_string(),
        }
    }
}
fn empty_json_parameter() -> String {
    "{}".to_string()
}
