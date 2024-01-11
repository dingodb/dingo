use roaring::RoaringBitmap;
use std::sync::Arc;
use tantivy::query::{QueryParser, RegexQuery};

use crate::commons::CACHE_FOR_SKIP_INDEX;
use crate::commons::LOG_CALLBACK;
use crate::logger::ffi_logger::callback_with_thread_info;
use crate::ERROR;

use super::index_r::*;
use super::row_id_bitmap_collector::RowIdRoaringCollector;

fn compute_bitmap(index_r: &IndexR, query_str: &str, use_regrex: bool) -> Arc<RoaringBitmap> {
    let schema = index_r.reader.searcher().index().schema();
    let text = match schema.get_field("text") {
        Ok(str) => str,
        Err(_) => {
            ERROR!("Missing text field.");
            return Arc::new(RoaringBitmap::new());
        }
    };

    let searcher = index_r.reader.searcher();
    let row_id_collector = RowIdRoaringCollector::with_field("row_id".to_string());

    if use_regrex {
        let regex_query = match RegexQuery::from_pattern(&like_to_regex(query_str), text) {
            Ok(parsed_query) => parsed_query,
            Err(e) => {
                ERROR!(
                    "Can't parse regrex query: {}, {}",
                    like_to_regex(query_str),
                    e
                );
                return Arc::new(RoaringBitmap::new());
            }
        };
        let searched_bitmap = match searcher.search(&regex_query, &row_id_collector) {
            Ok(result_bitmap) => result_bitmap,
            Err(e) => {
                ERROR!("Can't execute search in `compute_bitmap`: {}", e);
                return Arc::new(RoaringBitmap::new());
            }
        };
        searched_bitmap
    } else {
        let query_parser = QueryParser::for_index(index_r.reader.searcher().index(), vec![text]);
        let text_query = match query_parser.parse_query(query_str) {
            Ok(parsed_query) => parsed_query,
            Err(e) => {
                ERROR!("Can't parse query: {}, error:{}", query_str, e.to_string());
                return Arc::new(RoaringBitmap::new());
            }
        };
        let searched_bitmap = match searcher.search(&text_query, &row_id_collector) {
            Ok(result_bitmap) => result_bitmap,
            Err(e) => {
                ERROR!("Can't execute search in `compute_bitmap`: {}", e);
                return Arc::new(RoaringBitmap::new());
            }
        };
        searched_bitmap
    }
}

fn intersect_and_return(
    row_id_roaring_bitmap: Arc<RoaringBitmap>,
    lrange: u64,
    rrange: u64,
) -> Result<Arc<RoaringBitmap>, String> {
    let mut row_id_range = RoaringBitmap::new();
    row_id_range.insert_range(lrange as u32..(rrange + 1) as u32);
    row_id_range &= Arc::as_ref(&row_id_roaring_bitmap);
    Ok(Arc::new(row_id_range))
}

/// Performs a search operation using the given index reader, query, and range.
///
/// Arguments:
/// - `index_r`: refrence to `IndexR`.
/// - `query_str`: query string.
/// - `lrange`: The lower bound of the row ID range.
/// - `rrange`: The upper bound of the row ID range.
/// - `use_regrex`: Whether to use regex search.
///
/// Returns:
/// - `Result<RoaringBitmap, String>`: A `RoaringBitmap` containing the search results,
///   or an error if the search fails.
pub fn perform_search(
    index_r: &IndexR,
    query_str: &str,
    lrange: u64,
    rrange: u64,
    use_regrex: bool,
) -> Result<Arc<RoaringBitmap>, String> {
    #[cfg(feature = "use-flurry-cache")]
    {
        // Resolve cache or compute the roaring bitmap for the given query.
        let row_id_roaring_bitmap = CACHE_FOR_SKIP_INDEX.resolve(
            (
                index_r.reader_address(),
                query_str.to_string(),
                index_r.path.to_string(),
            ),
            || compute_bitmap(index_r, query_str, use_regrex),
        );
        intersect_and_return(row_id_roaring_bitmap, lrange, rrange)
    }
    #[cfg(not(feature = "use-flurry-cache"))]
    {
        let row_id_roaring_bitmap = compute_bitmap(index_r, query_str, use_regrex);
        intersect_and_return(row_id_roaring_bitmap, lrange, rrange)
    }
}

// Convert Clickhouse like pattern to Rust regex pattern.
pub fn like_to_regex(like_pattern: &str) -> String {
    let mut regex_pattern = String::new();
    let mut escape = false;

    for c in like_pattern.chars() {
        match c {
            // got r'\', if not escape currently, need escape.
            '\\' if !escape => {
                escape = true;
            }

            // got r'\', if escaped currently, need push r'\\'
            '\\' if escape => {
                regex_pattern.push_str("\\\\");
                escape = false;
            }

            // In not escape mode, convert '%' to '.*'
            '%' if !escape => regex_pattern.push_str(".*"),

            // In not escape mode, convert '_' to '.'
            '_' if !escape => regex_pattern.push('.'),

            // In escape mode, handle '%'、'_'
            '%' | '_' if escape => {
                regex_pattern.push(c);
                escape = false;
            }

            // Handle regex special chars.
            _ => {
                if ".+*?^$()[]{}|".contains(c) {
                    regex_pattern.push('\\');
                }
                regex_pattern.push(c);
                escape = false;
            }
        }
    }

    regex_pattern
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_like_to_regex() {
        // testing normal strings
        assert_eq!(r"a\bc", "a\\bc");
        assert_eq!(like_to_regex("abc"), "abc");
        assert_eq!(like_to_regex(r"ab\\c"), "ab\\\\c");

        // testing '%' conversion to '.*'
        assert_eq!(like_to_regex(r"a%b%c"), "a.*b.*c");

        // testing '_' conversion to '.'
        assert_eq!(like_to_regex(r"a_b_c"), "a.b.c");

        // testing conversion: '%' and '_'
        assert_eq!(like_to_regex("a\\%b\\_c"), "a%b_c");

        // testing consecutive '%' and '_'
        assert_eq!(like_to_regex(r"%%__"), ".*.*..");

        // testing escape sequences
        assert_eq!(like_to_regex("a\\%b%c\\_d"), "a%b.*c_d");

        // testing escaped '\'
        assert_eq!(like_to_regex("%\\\\%"), ".*\\\\.*");

        // testing special cases such as empty strings
        assert_eq!(like_to_regex(""), "");

        // testing special characters in regular expressions
        assert_eq!(like_to_regex("%a.b[c]%"), ".*a\\.b\\[c\\].*");

        // testing combinations of escaped and unescaped characters.
        assert_eq!(like_to_regex("a%b_c\\%d\\_e\\\\"), "a.*b.c%d_e\\\\");
    }
}
