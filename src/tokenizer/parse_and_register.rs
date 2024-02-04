use cang_jie::{CangJieTokenizer, TokenizerOption};
use jieba_rs::Jieba;
use regex::Regex;
use std::str::FromStr;
use std::sync::Arc;
use tantivy::{
    tokenizer::{
        Language, LowerCaser, NgramTokenizer, RawTokenizer, RemoveLongFilter, SimpleTokenizer,
        Stemmer, StopWordFilter, TextAnalyzer, WhitespaceTokenizer,
    },
    Index,
};

#[derive(Clone)]
pub enum TokenizerType {
    Default(String),
    Raw(String),
    Simple(String),
    EnStem(String),
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
            TokenizerType::EnStem(name) => name,
            TokenizerType::WhiteSpace(name) => name,
            TokenizerType::Ngram(name) => name,
            TokenizerType::Chinese(name) => name,
        }
    }
}

// Register the third-party tokenizer to the index
pub fn register_tokenizer_to_index(
    index: &mut Index,
    tokenizer_type: TokenizerType,
    tokenizer: TextAnalyzer,
) -> Result<String, String> {
    #[allow(unreachable_patterns)]
    match tokenizer_type {
        TokenizerType::Default(tokenizer_name) => {
            index.tokenizers().register(&tokenizer_name, tokenizer);
            Ok("`default` tokenizer has been registered".to_string())
        }
        TokenizerType::Raw(tokenizer_name) => {
            index.tokenizers().register(&tokenizer_name, tokenizer);
            Ok("`raw` tokenizer has been registered".to_string())
        }
        TokenizerType::Simple(tokenizer_name) => {
            index.tokenizers().register(&tokenizer_name, tokenizer);
            Ok("`simple` tokenizer has been registered".to_string())
        }
        TokenizerType::EnStem(tokenizer_name) => {
            index.tokenizers().register(&tokenizer_name, tokenizer);
            Ok("`en_stem` tokenizer has been registered".to_string())
        }
        TokenizerType::WhiteSpace(tokenizer_name) => {
            index.tokenizers().register(&tokenizer_name, tokenizer);
            Ok("`whitespace` tokenizer has been registered".to_string())
        }
        TokenizerType::Ngram(tokenizer_name) => {
            index.tokenizers().register(&tokenizer_name, tokenizer);
            Ok("`ngram` tokenizer has been registered".to_string())
        }
        TokenizerType::Chinese(tokenizer_name) => {
            index.tokenizers().register(&tokenizer_name, tokenizer);
            Ok("`chinese` tokenizer has been registered".to_string())
        }
        _ => Err("unknown tokenizer type.".to_string()),
    }
}

// TODO：refine stop words language
pub fn get_custom_tokenizer(
    tokenizer_with_parameter: &str,
) -> Result<(TokenizerType, TextAnalyzer), String> {
    let tokenizer_with_parameter_lower: String =
        tokenizer_with_parameter.to_string().to_lowercase();
    // let re = match Regex::new(r"([a-zA-Z_]+)(?:\((.*?)\))?") {
    // let re = match Regex::new(r"([^\s(]+)(?:\((.*?)\))?") {
    // let re = match Regex::new(r"([^(]+)(?:\((.*?)\))?") {
    // let re = match Regex::new(r"^([^(]*)(?:\((.*?)\))?") {
    let re = match Regex::new(r"^([^(]*)(?:\((.*)\))?") {
        Ok(regex) => regex,
        Err(e) => return Err(format!("Regex pattern initialize error: {}", e)),
    };
    let caps = match re.captures(&tokenizer_with_parameter_lower) {
        Some(caps) => caps,
        None => {
            return Err(format!(
                "Invalid tokenizer format: {}",
                tokenizer_with_parameter_lower
            ))
        }
    };

    let tokenizer_name: &str = caps.get(1).map_or("default", |m| m.as_str());
    let params: Vec<&str> = caps.get(2).map_or(Vec::new(), |m| {
        m.as_str()
            .split(',')
            .map(|s| s.trim()) // remove white space
            .collect()
    });

    match tokenizer_name {
        "default" => {
            // Parameter general check
            if !params.is_empty() {
                return Err(format!(
                    "`default` tokenizer doesn't support any parameter, but given: {:?}.",
                    params
                ));
            }
            let tokenizer: (TokenizerType, TextAnalyzer) = (
                TokenizerType::Default("default".to_string()),
                TextAnalyzer::builder(SimpleTokenizer::default())
                    .filter(RemoveLongFilter::limit(40))
                    .filter(LowerCaser)
                    .build(),
            );
            Ok(tokenizer)
        }
        "raw" => {
            // Parameter general check
            if !params.is_empty() {
                return Err(format!(
                    "`raw` tokenizer doesn't support any parameter, but given: {:?}",
                    params
                ));
            }
            let tokenizer: (TokenizerType, TextAnalyzer) = (
                TokenizerType::Raw("raw".to_string()),
                TextAnalyzer::builder(RawTokenizer::default()).build(),
            );
            Ok(tokenizer)
        }
        "simple" => {
            // Parameter general check
            if !params.is_empty() && params.len() > 1 {
                return Err(format!("`simple` tokenizer supports one bool parameter: `use_stop_words`, but given: {:?}", params));
            }
            // Check 1st parameter
            if let Some(&param) = params.get(0) {
                if param != "true" && param != "false" {
                    return Err(format!("`simple` tokenizer parameter `use_stop_words` must be 'true' or 'false', but given: {}", param));
                }
            }
            let use_stop_words = params.get(0).map_or(false, |&s| s == "true");

            let builder = TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(RemoveLongFilter::limit(40))
                .filter(LowerCaser);

            if use_stop_words {
                Ok((
                    TokenizerType::Simple("simple".to_string()),
                    builder
                        .filter(StopWordFilter::new(Language::English).unwrap())
                        .build(),
                ))
            } else {
                Ok((TokenizerType::Simple("simple".to_string()), builder.build()))
            }
        }
        "en_stem" => {
            // Parameter general check
            if !params.is_empty() && params.len() > 1 {
                return Err(format!("`en_stem` tokenizer supports one bool parameter: `use_stop_words`, but given: {:?}", params));
            }
            // Check 1st parameter
            if let Some(&param) = params.get(0) {
                if param != "true" && param != "false" {
                    return Err(format!("`en_stem` tokenizer parameter `use_stop_words` must be 'true' or 'false', but given: {}", param));
                }
            }
            let use_stop_words = params.get(0).map_or(false, |&s| s == "true");

            let builder = TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(RemoveLongFilter::limit(40))
                .filter(LowerCaser)
                .filter(Stemmer::new(Language::English));

            if use_stop_words {
                Ok((
                    TokenizerType::EnStem("en_stem".to_string()),
                    builder
                        .filter(StopWordFilter::new(Language::English).unwrap())
                        .build(),
                ))
            } else {
                Ok((
                    TokenizerType::EnStem("en_stem".to_string()),
                    builder.build(),
                ))
            }
        }
        "whitespace" => {
            // Parameter general check
            if !params.is_empty() && params.len() > 1 {
                return Err(format!("`whitespace` tokenizer supports one bool parameter: `use_stop_words`, but given: {:?}", params));
            }
            // Check 1st parameter
            if let Some(&param) = params.get(0) {
                if param != "true" && param != "false" {
                    return Err(format!("`whitespace` tokenizer parameter `use_stop_words` must be 'true' or 'false', but given: {}", param));
                }
            }
            let use_stop_words = params.get(0).map_or(false, |&s| s == "true");

            let builder = TextAnalyzer::builder(WhitespaceTokenizer::default())
                .filter(RemoveLongFilter::limit(40))
                .filter(LowerCaser);

            if use_stop_words {
                Ok((
                    TokenizerType::WhiteSpace("whitespace".to_string()),
                    builder
                        .filter(StopWordFilter::new(Language::English).unwrap())
                        .build(),
                ))
            } else {
                Ok((
                    TokenizerType::WhiteSpace("whitespace".to_string()),
                    builder.build(),
                ))
            }
        }
        // for ngram, your text options need postions information
        "ngram" => {
            // Parameter general check
            if !params.is_empty() && params.len() > 4 {
                return Err(format!("`ngram` tokenizer supports parameter: [`use_stop_words`, `min_gram`, `max_gram`, `prefix_only`], but given: {:?}", params));
            }
            // Check 1st parameter
            if let Some(&param) = params.get(0) {
                if param != "true" && param != "false" {
                    return Err(format!("`ngram` tokenizer parameter `use_stop_words` must be 'true' or 'false', but given: {}", param));
                }
            }
            // Check 2nd parameter
            if let Some(&param) = params.get(1) {
                if param.parse::<usize>().is_err() {
                    return Err(format!(
                        "`ngram` tokenizer parameter `min_gram` must be an integer, but given: {}",
                        param
                    ));
                }
            }
            // Check 3rd parameter
            if let Some(&param) = params.get(2) {
                if param.parse::<usize>().is_err() {
                    return Err(format!(
                        "`ngram` tokenizer parameter `max_gram` must be an integer, but given: {}",
                        param
                    ));
                }
            }
            // Check 4th parameter
            if let Some(&param) = params.get(3) {
                if param != "true" && param != "false" {
                    return Err(format!("`ngram` tokenizer parameter `prefix_only` must be 'true' or 'false', but given: {}", param));
                }
            }

            let use_stop_words = params.get(0).map_or(false, |&s| s == "true");
            let min_gram = params
                .get(1)
                .and_then(|&s| usize::from_str(s).ok())
                .unwrap_or(2);
            let max_gram = params
                .get(2)
                .and_then(|&s| usize::from_str(s).ok())
                .unwrap_or(3);
            let prefix_only = params.get(3).map_or(false, |&s| s == "true");

            if min_gram > max_gram {
                return Err(format!(
                    "`ngram` tokenizer parameter `min_gram` ({}) must be less than `max_gram` ({})",
                    min_gram, max_gram
                ));
            }

            let builder = TextAnalyzer::builder(
                NgramTokenizer::new(min_gram, max_gram, prefix_only).unwrap(),
            )
            .filter(RemoveLongFilter::limit(40))
            .filter(LowerCaser);

            if use_stop_words {
                Ok((
                    TokenizerType::Ngram("ngram".to_string()),
                    builder
                        .filter(StopWordFilter::new(Language::English).unwrap())
                        .build(),
                ))
            } else {
                Ok((TokenizerType::Ngram("ngram".to_string()), builder.build()))
            }
        }
        "chinese" => {
            // Parameter general check
            if !params.is_empty() && params.len() > 3 {
                return Err(format!("`chinese` tokenizer supports parameter: [`jieba_mode`, `cangjie_mode`, `cangjie_hmm`], but given: {:?}", params));
            }
            // Check 1st parameter
            if let Some(&param) = params.get(0) {
                if param != "default" && param != "empty" {
                    return Err(format!("`chinese` tokenizer parameter `jieba_mode` must be 'default' or 'empty', but given: {}", param));
                }
            }
            // Check 2nd parameter
            if let Some(&param) = params.get(1) {
                if param != "all" && param != "unicode" && param != "default" && param != "search" {
                    return Err(format!("`chinese` tokenizer parameter `cangjie_mode` must be one of [`all`、`unicode`、`default`、`search`], but given: {}", param));
                }
            }
            // Check 3rd parameter
            if let Some(&param) = params.get(2) {
                if param != "true" && param != "false" {
                    return Err(format!("`chinese` tokenizer parameter `jieba_mode` must be 'default' or 'empty', but given: {}", param));
                }
            }
            let jieba_mode = params.get(0).map_or(Jieba::empty(), |&s| {
                if s == "default" {
                    Jieba::default()
                } else {
                    Jieba::empty()
                }
            });
            let use_hmm = params.get(2).map_or(false, |&s| s == "true");

            let cangjie_option = params.get(1).map_or(TokenizerOption::Unicode, |s| {
                match *s {
                    "all" => TokenizerOption::All,
                    "unicode" => TokenizerOption::Unicode,
                    "default" => TokenizerOption::Default { hmm: use_hmm },
                    "search" => TokenizerOption::ForSearch { hmm: use_hmm },
                    _ => TokenizerOption::Unicode, // default option
                }
            });

            Ok((
                TokenizerType::Chinese("chinese".to_string()),
                TextAnalyzer::builder(CangJieTokenizer {
                    worker: Arc::new(jieba_mode),
                    option: cangjie_option,
                })
                .build(),
            ))
        }
        _ => Err(format!("Unknown tokenizer type: {}.", tokenizer_name)),
    }
}

#[cfg(test)]
mod tests {
    use env_logger::Env;
    use rstest::rstest;
    use tantivy::{
        collector::Count,
        query::{QueryParser, RegexQuery},
        schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, FAST, INDEXED},
        Document, Searcher,
    };

    use super::*;

    #[rstest]
    #[case("raw", ".*🚀.*", 1, "of", 0, "\"just for raw\"", 1)]
    #[case("simple", ".*🚀.*", 0, "of", 2, "rise and fall", 1)]
    #[case("simple(false)", ".*🚀.*", 0, "of", 2, "rise and fall", 1)]
    #[case("simple(true)", ".*🚀.*", 0, "of", 0, "rise and fall", 1)]
    #[case("en_stem", ".*🚀.*", 0, "of", 2, "rise and fall", 1)]
    #[case("en_stem(false)", ".*🚀.*", 0, "of", 2, "rise and fall", 1)]
    #[case("en_stem(true)", ".*🚀.*", 0, "of", 0, "rise and fall", 1)]
    #[case("whitespace", ".*🚀.*", 1, "of", 2, "rise and fall", 1)]
    #[case("whitespace(false)", ".*🚀.*", 1, "of", 2, "rise and fall", 1)]
    #[case("whitespace(true)", ".*🚀.*", 1, "of", 0, "rise and fall", 1)]
    #[case("ngram", ".*🚀.*", 1, "of", 2, "rise and fall", 1)]
    #[case("ngram(false)", ".*🚀.*", 1, "of", 2, "rise and fall", 1)]
    #[case("ngram(false, 2)", ".*🚀.*", 1, "of", 2, "rise and fall", 1)]
    #[case("ngram(false,2, 4)", ".*🚀.*", 1, "of", 2, "rise and fall", 1)]
    #[case("ngram(true,2, 4)", ".*🚀.*", 1, "of", 0, "rise and fall", 1)]
    #[case("ngram(false,1 , 5, true)", ".*🚀.*", 0, "of", 0, "rise", 0)]
    #[case("chinese", ".*相识.*", 0, "与你相识 很值", 2, "飞 驰", 1)] // 中英逐个字符分词
    #[case("chinese(empty)", ".*相识.*", 0, "与你相识 很值", 2, "飞 驰", 1)] // 中英逐个字符分词
    #[case("chinese(default)", ".*相识.*", 0, "与你相识 很值", 2, "飞 驰", 1)] // 中英逐个字符分词
    #[case("chinese(empty, all)", ".*相识.*", 0, "与你相识", 0, "飞驰", 0)] // 所有的中文字符在分词之后都会被剔除
    #[case("chinese(default, all)", ".*相识.*", 1, "与你相识 很值", 2, "飞 驰", 1)]
    // 分词很啰嗦, 比如 "下午" -> ["下", "下午", "午"]；"飞驰"->["飞", "飞驰", "驰"] 但是 "飞驰" 没有命中结果, 分开才可以
    #[case(
        "chinese(default, default)",
        ".*相识.*",
        1,
        "与你相识 很值",
        1,
        "飞 驰",
        0
    )]
    // "与你相识"->["与", "你", "相识"]; "很值"->["很", "值"] 虽然两句话都有 "值" 但看起来都没有命中，估计命中的是 "相识"
    #[case(
        "chinese(default, default, true)",
        ".*相识.*",
        1,
        "与你相识 很值",
        1,
        "飞 驰",
        0
    )]
    // "与你相识"->["与", "你", "相识"]; "很值"->["很值"] 同时对 search 和入库的字符串都是用 cut 模式
    #[case(
        "chinese(default, search)",
        ".*相识.*",
        1,
        "与你相识 很值",
        1,
        "飞 驰",
        0
    )]
    // "与你相识"->["与", "你", "相识"]; "很值"->["很", "值"] 只对 search 的字符串使用 cut 模式, 即文字之间不会重复
    #[case(
        "chinese(default, search, true)",
        ".*相识.*",
        1,
        "与你相识 很值",
        1,
        "飞 驰",
        0
    )] // "与你相识"->["与", "你", "相识"]; "很值"->["很值"]
    #[case(
        "chinese(default, unicode)",
        ".*相识.*",
        0,
        "与你相识 很值",
        2,
        "飞 驰",
        1
    )] // "与你相识"->["与", "你", "相", "识"]; "很值"->["很", "值"]
    fn test_get_custom_tokenizer_behavior(
        #[case] tokenizer_name: &str,
        #[case] regex_query: &str,
        #[case] regex_expect_count: usize,
        #[case] stop_word_query: &str,
        #[case] stop_word_expect_count: usize,
        #[case] normal_query: &str,
        #[case] normal_expect_count: usize,
    ) {
        let (schema, searcher) = initialize_custom_tokenizer(tokenizer_name);
        let text_field = schema.get_field("text").unwrap();
        validate_regex_query_with_custom_tokenizer(
            &searcher,
            regex_query,
            text_field,
            regex_expect_count,
        );
        validate_normal_query_with_custom_tokenizer(
            &searcher,
            stop_word_query,
            text_field,
            stop_word_expect_count,
        );
        validate_normal_query_with_custom_tokenizer(
            &searcher,
            normal_query,
            text_field,
            normal_expect_count,
        );
    }

    #[test]
    fn test_get_custom_tokenizer_sample() {
        env_logger::Builder::from_env(Env::default().default_filter_or("trace")).init();

        // init tokenizer
        let (tokenizer_type, tokenizer) = get_custom_tokenizer("chinese(default, search)").unwrap();
        // get searcher
        let (schema, searcher) = test_get_custom_tokenizer_helper(tokenizer_type.name(), tokenizer);

        let text_field = schema.get_field("text").unwrap();

        let emoji_regex_query = RegexQuery::from_pattern(".*rise.*", text_field).unwrap();
        // let emoji_regex_query = RegexQuery::from_regex(Regex::new("jap[A-Z]n").unwrap(), text_field);

        let emoji_regex_count = searcher
            .search(&emoji_regex_query, &Count)
            .expect("failed to execute regex search with emoji");
        println!("emoji_regex_count:{}", emoji_regex_count);
        // assert_eq!(emoji_regex_count, 1);

        let query_parser = QueryParser::for_index(&searcher.index(), vec![text_field]);
        let stop_text_query = query_parser
            .parse_query("rise")
            .expect("failed to parse text query with raw");
        let stop_text_count = searcher
            .search(&stop_text_query, &Count)
            .expect("failed to execute text search with raw");
        println!("stop_text_count: {}", stop_text_count);

        let query_parser = QueryParser::for_index(&searcher.index(), vec![text_field]);
        let raw_text_query = query_parser
            .parse_query("rise and fall")
            .expect("failed to parse text query with raw");
        let raw_text_count = searcher
            .search(&raw_text_query, &Count)
            .expect("failed to execute text search with raw");
        println!("raw_text_count: {}", raw_text_count);

        let query_parser = QueryParser::for_index(&searcher.index(), vec![text_field]);
        let multi_chinese_words = query_parser
            .parse_query("差旅 OR (日常 AND 生活)")
            .expect("failed to parse text query with raw");
        let multi_chinese_words_count = searcher
            .search(&multi_chinese_words, &Count)
            .expect("failed to execute text search with chinese words");
        println!("multi_chinese_words_count: {}", multi_chinese_words_count);
        // assert_eq!(raw_text_count, 0);
    }

    fn test_get_custom_tokenizer_helper(
        tokenizer_name: &str,
        tokenizer: TextAnalyzer,
    ) -> (Schema, Searcher) {
        // create text_options with TextAnalyzer name
        let text_options = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(tokenizer_name)
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        );

        // create schema
        let mut schema_builder = Schema::builder();
        let row_id = schema_builder.add_u64_field("row_id", FAST | INDEXED);
        let text = schema_builder.add_text_field("text", text_options);
        let schema = schema_builder.build();

        // create index and regist tokenizer
        let index = Index::create_in_ram(schema.clone());
        index.tokenizers().register(tokenizer_name, tokenizer);

        // write docs
        let mut index_writer = index.writer_with_num_threads(1, 1024 * 1024 * 32).unwrap();
        let str_vec: Vec<String> = vec![
            r"Ancient empires rise and fall, shaping 🐶 history's course.".to_string(),
            r"Artistic expres🐶sio$ns reflect diver$¥se cu\\ltural heritages.".to_string(),
            r"Social move?ments transform soc>iet不好%你ies, forging new paths.".to_string(),
            r"Eco$nomies🐶 fluctuate, % reflecting the complex interplay of global forces."
                .to_string(),
            r"Strategic military 🐶%🐈 camp%aigns alter the bala🚀nce of power.\%".to_string(),
            r"just for raw".to_string(),
            r"我姓石，无论何时与你相识我都值".to_string(),
            r"心往神驰执笔在意写神池".to_string(),
            r"你今天下午吃饭了吗？晚上很值得吃一顿！".to_string(),
            r"这是你的差旅账单".to_string(),
            r"这是你的日常账单".to_string(),
            r"这是你的生活账单".to_string(),
        ];
        for i in 0..str_vec.len() {
            let mut temp = Document::default();
            temp.add_u64(row_id, i as u64);
            temp.add_text(text, &str_vec[i]);
            let _ = index_writer.add_document(temp);
        }
        index_writer.commit().unwrap();

        // return searcher
        let reader = index.reader().unwrap();
        (schema, reader.searcher())
    }

    fn initialize_custom_tokenizer(tokenizer_param_str: &str) -> (Schema, Searcher) {
        let (tokenizer_type, tokenizer) = get_custom_tokenizer(tokenizer_param_str).unwrap();
        test_get_custom_tokenizer_helper(tokenizer_type.name(), tokenizer)
    }

    fn validate_normal_query_with_custom_tokenizer(
        searcher: &Searcher,
        query: &str,
        field: Field,
        expected_count: usize,
    ) {
        let query_parser = QueryParser::for_index(&searcher.index(), vec![field]);
        let parsed_query = query_parser
            .parse_query(query)
            .expect("failed to parse query");
        let count = searcher
            .search(&parsed_query, &Count)
            .expect("failed to execute search");
        assert_eq!(count, expected_count);
    }

    fn validate_regex_query_with_custom_tokenizer(
        searcher: &Searcher,
        query: &str,
        field: Field,
        expected_count: usize,
    ) {
        let parsed_query =
            RegexQuery::from_pattern(query, field).expect("failed to parse from regex pattern");
        let count = searcher
            .search(&parsed_query, &Count)
            .expect("failed to execute regex search");
        assert_eq!(count, expected_count);
    }
}
