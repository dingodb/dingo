#[cfg(test)]
mod tests{
    use std::fs;

    use cxx::let_cxx_string;
    use cxx::*;
    use tempfile::TempDir;

    use crate::tantivy_create_index_with_tokenizer;

    #[test]
    pub fn test_create_index_with_valid_tokenizer(){
        let temp_directory = TempDir::new().expect("Can't create temp directory");
        let_cxx_string!(tokenizer_with_parameter = "whitespace(false)");
        let_cxx_string!(index_directory = temp_directory.path().to_str().expect("Can't convert temp directory to temp directory_str"));
        let tokenizer_with_parameter_cxx: &CxxString = tokenizer_with_parameter.as_ref().get_ref();
        let index_directory_cxx: &CxxString = index_directory.as_ref().get_ref();
        let result = tantivy_create_index_with_tokenizer(index_directory_cxx, tokenizer_with_parameter_cxx, false);
        assert!(result.is_ok());
    }

    #[test]
    pub fn test_create_index_with_invalid_tokenizer(){
        let temp_directory = TempDir::new().expect("Can't create temp directory");
        let_cxx_string!(tokenizer_with_parameter = "default(ABC)");
        let_cxx_string!(index_directory = temp_directory.path().to_str().expect("Can't convert temp directory to temp directory_str"));
        let tokenizer_with_parameter_cxx: &CxxString = tokenizer_with_parameter.as_ref().get_ref();
        let index_directory_cxx: &CxxString = index_directory.as_ref().get_ref();
        let result = tantivy_create_index_with_tokenizer(index_directory_cxx, tokenizer_with_parameter_cxx, false);
        assert!(result.is_err());
    }

    #[test]
    pub fn test_create_index_with_not_empty_directory(){
        let temp_directory = TempDir::new().expect("Can't create temp directory");
        let temp_directory_str = temp_directory.path().to_str().unwrap();
        // Create some cxx parameters.
        let_cxx_string!(tokenizer_with_parameter = "default");
        let_cxx_string!(index_directory = temp_directory_str);
        let tokenizer_with_parameter_cxx: &CxxString = tokenizer_with_parameter.as_ref().get_ref();
        let index_directory_cxx: &CxxString = index_directory.as_ref().get_ref();
        // Create index in a clean directory.
        assert!(tantivy_create_index_with_tokenizer(index_directory_cxx, tokenizer_with_parameter_cxx, false).is_ok());
        // Create index in a not empty directory.
        assert!(tantivy_create_index_with_tokenizer(index_directory_cxx, tokenizer_with_parameter_cxx, false).is_ok());
    }
}