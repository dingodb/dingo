use roaring::RoaringBitmap;
use std::sync::Arc;
use tantivy::query::{QueryParser, RegexQuery};

use super::bridge::index_reader_bridge::IndexReaderBridge;
use super::collector::row_id_bitmap_collector::RowIdRoaringCollector;
use crate::common::constants::CACHE_FOR_SKIP_INDEX;
use crate::logger::ffi_logger::callback_with_thread_info;
use crate::{common::constants::LOG_CALLBACK, ERROR};

fn compute_bitmap(index_r: &IndexReaderBridge, query_str: &str, use_regex: bool) -> Arc<RoaringBitmap> {
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

    if use_regex {
        let regex_query = match RegexQuery::from_pattern(&like_to_regex(query_str), text) {
            Ok(parsed_query) => parsed_query,
            Err(e) => {
                ERROR!(
                    "Can't parse regex query: {}, {}",
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
                ERROR!("Can't parse query: {}, due to:{}", query_str, e.to_string());
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
    let lrange_u32: u32 = lrange
        .try_into()
        .map_err(|_| "lrange value is too large and causes u32 overflow")?;
    let rrange_u32: u32 = rrange
        .try_into()
        .map_err(|_| "rrange value is too large and causes u32 overflow")?;
    let rrange_plus_one_u32 = rrange_u32
        .checked_add(1)
        .ok_or("rrange + 1 causes u32 overflow")?;

    let mut row_id_range = RoaringBitmap::new();
    row_id_range.insert_range(lrange_u32..rrange_plus_one_u32);
    row_id_range &= Arc::as_ref(&row_id_roaring_bitmap);
    Ok(Arc::new(row_id_range))
}

/// Performs a search operation using the given index reader, query, and range.
///
/// Arguments:
/// - `index_r`: refrence to `IndexReaderBridge`.
/// - `query_str`: query string.
/// - `lrange`: The lower bound of the row ID range.
/// - `rrange`: The upper bound of the row ID range.
/// - `use_regex`: Whether to use regex search.
///
/// Returns:
/// - `Result<RoaringBitmap, String>`: A `RoaringBitmap` containing the search results,
///   or an error if the search fails.
pub fn perform_search_with_range(
    index_r: &IndexReaderBridge,
    query_str: &str,
    lrange: u64,
    rrange: u64,
    use_regex: bool,
) -> Result<Arc<RoaringBitmap>, String> {
    let row_ids = match perform_search(index_r, query_str, use_regex) {
        Ok(content) => content,
        Err(e) => {
            let error_info = format!("Error in perform_search: {}", e);
            ERROR!("{}", error_info);
            return Err(error_info);
        }
    };
    intersect_and_return(row_ids, lrange, rrange)
}

pub fn perform_search(
    index_r: &IndexReaderBridge,
    query_str: &str,
    use_regex: bool,
) -> Result<Arc<RoaringBitmap>, String> {
    #[cfg(feature = "use-flurry-cache")]
    {
        // Resolve cache or compute the roaring bitmap for the given query.
        Ok(CACHE_FOR_SKIP_INDEX.resolve(
            (
                index_r.reader_address(),
                query_str.to_string(),
                index_r.path.to_string(),
                use_regex,
            ),
            || compute_bitmap(index_r, query_str, use_regex),
        ))
    }
    #[cfg(not(feature = "use-flurry-cache"))]
    {
        Ok(compute_bitmap(index_r, query_str, use_regex))
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

// convert u8_bitmap to row_ids
pub fn u8_bitmap_to_row_ids(bitmap: &[u8]) -> Vec<u32> {
    let mut row_ids = Vec::new();
    for (i, &byte) in bitmap.iter().enumerate() {
        for j in 0..8 {
            if byte & (1 << j) != 0 {
                row_ids.push((i * 8 + j) as u32);
            }
        }
    }
    row_ids
}

pub fn row_ids_to_u8_bitmap(row_ids: &[u32]) -> Vec<u8> {
    // O(n) try get max row_id, we use it to caculate bitmap(u8 vec) size
    let max_row_id = match row_ids.iter().max() {
        Some(&max) => max,
        None => return Vec::new(),
    };
    let u8_bitmap_size = (max_row_id as usize / 8) + 1;
    let mut bitmap = vec![0u8; u8_bitmap_size];

    for &row_id in row_ids {
        let byte_index = (row_id / 8) as usize;
        let bit_index = row_id % 8;
        bitmap[byte_index] |= 1 << bit_index;
    }

    bitmap
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

    #[test]
    fn test_u8_bitmap_to_row_ids() {
        // empty bitmap
        let bitmap_empty: Vec<u8> = Vec::new();
        let row_ids_empty: Vec<u32> = Vec::new();
        assert_eq!(u8_bitmap_to_row_ids(&bitmap_empty), row_ids_empty);

        // bitmap with many zero
        let mut bitmap_a: Vec<u8> = vec![0, 0, 0, 0, 0];
        bitmap_a.extend(vec![0; 1000]);
        assert_eq!(u8_bitmap_to_row_ids(&bitmap_a), row_ids_empty);

        // full bitmap
        let bitmap_b: Vec<u8> = vec![255];
        assert_eq!(u8_bitmap_to_row_ids(&bitmap_b), [0, 1, 2, 3, 4, 5, 6, 7]);

        let bitmap_c: Vec<u8> = vec![255, 255];
        assert_eq!(
            u8_bitmap_to_row_ids(&bitmap_c),
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        );

        // 00001100, 00010000
        let bitmap_d: Vec<u8> = vec![12, 16];
        assert_eq!(u8_bitmap_to_row_ids(&bitmap_d), [2, 3, 12]);

        // 00000001, 00000000, 00000010, 00000100
        let bitmap_e: Vec<u8> = vec![1, 0, 2, 4];
        assert_eq!(u8_bitmap_to_row_ids(&bitmap_e), [0, 17, 26]);

        // 00100010, 01000001, 10000000
        let bitmap_f: Vec<u8> = vec![34, 65, 128];
        assert_eq!(u8_bitmap_to_row_ids(&bitmap_f), [1, 5, 8, 14, 23]);

        // large u8 bitmap, contains 8 element.
        let bitmap_g: Vec<u8> = vec![
            0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000, 0b00100000, 0b01000000,
            0b10000000, 0b00000000, 0b00000000,
        ];
        assert_eq!(
            u8_bitmap_to_row_ids(&bitmap_g),
            [0, 9, 18, 27, 36, 45, 54, 63]
        );

        let bitmap_h: Vec<u8> = vec![0, 32];
        assert_eq!(u8_bitmap_to_row_ids(&bitmap_h), [13]);
    }

    #[test]
    fn test_row_ids_to_u8_bitmap() {
        // empty bitmap
        let bitmap_empty: Vec<u8> = Vec::new();
        let row_ids_empty: Vec<u32> = Vec::new();
        assert_eq!(row_ids_to_u8_bitmap(&row_ids_empty), bitmap_empty);

        // row ids with many zero
        let mut row_ids_a: Vec<u32> = vec![0, 0, 0, 0, 0];
        row_ids_a.extend(vec![0; 1000]);
        assert_eq!(row_ids_to_u8_bitmap(&row_ids_a), [1]);

        // rowids can convert to full bitmap
        let row_ids_b: Vec<u32> = vec![0, 1, 2, 3, 4, 5, 6, 7];
        assert_eq!(row_ids_to_u8_bitmap(&row_ids_b), [255]);

        let row_ids_c: Vec<u32> = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        assert_eq!(row_ids_to_u8_bitmap(&row_ids_c), [255, 255]);

        // 00001100, 00010000
        let row_ids_d: Vec<u32> = vec![2, 3, 12];
        assert_eq!(row_ids_to_u8_bitmap(&row_ids_d), [12, 16]);

        // 00000001, 00000000, 00000010, 00000100
        let row_ids_e: Vec<u32> = vec![0, 17, 26];
        assert_eq!(row_ids_to_u8_bitmap(&row_ids_e), [1, 0, 2, 4]);

        // 00100010, 01000001, 10000000
        let row_ids_f: Vec<u32> = vec![1, 5, 8, 14, 23];
        assert_eq!(row_ids_to_u8_bitmap(&row_ids_f), [34, 65, 128]);

        // 8 rowids.
        let row_ids_g: Vec<u32> = vec![0, 9, 18, 27, 36, 45, 54, 63];
        assert_eq!(
            row_ids_to_u8_bitmap(&row_ids_g),
            [
                0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000, 0b00100000, 0b01000000,
                0b10000000,
            ]
        );

        let row_ids_h: Vec<u32> = vec![13];
        assert_eq!(row_ids_to_u8_bitmap(&row_ids_h), [0, 32]);
    }
}
