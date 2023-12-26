// SPDX-License-Identifier: Apache-2.0

#ifndef TANTIVY_SEARCH_H
#define TANTIVY_SEARCH_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

struct TantivySearchIndexR;

struct TantivySearchIndexW;

using TantivySearchLogCallback = void(*)(int32_t, const char*, const char*, const char*);

extern "C" {

/// Initializes the logger configuration for the tantivy search library.
///
/// Arguments:
/// - `log_path`: The path where log files are saved. Tantivy-search will generate multiple log files.
/// - `log_level`: The logging level to use. Supported levels: info, debug, trace, error, warning.
///   Note: 'fatal' is treated as 'error'.
/// - `console_logging`: Enables logging to the console if set to true.
/// - `callback`: A callback function, typically provided by ClickHouse.
/// - `enable_callback`: Enables the use of the callback function if set to true.
/// - `only_tantivy_search`: Only display `tantivy_search` log content.
///
/// Returns:
/// - `true` if the logger is successfully initialized, `false` otherwise.
bool tantivy_logger_initialize(const char *log_path,
                               const char *log_level,
                               bool console_logging,
                               TantivySearchLogCallback callback,
                               bool enable_callback,
                               bool only_tantivy_search);

/// Creates an index using a specified language (e.g., Chinese, English, Japanese, etc.).
///
/// Arguments:
/// - `dir_ptr`: The directory path for building the index.
/// - `language`: The language to be used.
///
/// Returns:
/// - A pointer to the created `IndexW`, or a null pointer if an error occurs.
TantivySearchIndexW *tantivy_create_index_with_language(const char *dir_ptr, const char *language);

/// Creates an index using the default language.
///
/// Arguments:
/// - `dir_ptr`: A pointer to the directory path where the index files will be created.
///
/// Returns:
/// - A pointer to `IndexW`, which encapsulates the index and writer.
/// - Returns a null pointer if any error occurs during the process.
TantivySearchIndexW *tantivy_create_index(const char *dir_ptr);

/// Loads an index from a specified directory.
///
/// Arguments:
/// - `dir_ptr`: A pointer to the directory path where the index files are located.
///
/// Returns:
/// - A pointer to `IndexR`, which encapsulates the loaded index and reader.
/// - Returns a null pointer if any error occurs during the loading process.
TantivySearchIndexR *tantivy_load_index(const char *dir_ptr);

/// Determines if a query string appears within a specified row ID range.
///
/// Arguments:
/// - `ir`: Pointer to the index reader.
/// - `query_ptr`: Pointer to the query string.
/// - `lrange`: The left (inclusive) boundary of the row ID range.
/// - `rrange`: The right (inclusive) boundary of the row ID range.
///
/// Returns:
/// - `true` if the query string appears in the given row ID range, `false` otherwise.
bool tantivy_search_in_rowid_range(TantivySearchIndexR *ir,
                                   const char *query_ptr,
                                   uint64_t lrange,
                                   uint64_t rrange,
                                   bool use_regrex);

/// Counts the occurrences of a query string within a specified row ID range.
///
/// Arguments:
/// - `ir`: Pointer to the index reader.
/// - `query_ptr`: Pointer to the query string.
/// - `lrange`: The left (inclusive) boundary of the row ID range.
/// - `rrange`: The right (inclusive) boundary of the row ID range.
///
/// Returns:
/// - The count of occurrences of the query string within the row ID range.
unsigned int tantivy_count_in_rowid_range(TantivySearchIndexR *ir,
                                          const char *query_ptr,
                                          uint64_t lrange,
                                          uint64_t rrange,
                                          bool use_regex);

/// Indexes a document.
///
/// Arguments:
/// - `iw`: Pointer to the index writer.
/// - `row_id_`: Row ID associated with the document.
/// - `text_`: Pointer to the text data of the document.
///
/// Returns:
/// - A non-zero value if successful, zero otherwise.
bool tantivy_index_doc(TantivySearchIndexW *iw, uint64_t row_id_, const char *text_);

/// Commits the changes to the index, writing it to the file system.
///
/// Arguments:
/// - `iw`: Pointer to the index writer.
///
/// Returns:
/// - A non-zero value if successful, zero otherwise.
bool tantivy_writer_commit(TantivySearchIndexW *iw);

/// Frees the index reader.
///
/// Arguments:
/// - `ir`: Pointer to the index reader.
void tantivy_reader_free(TantivySearchIndexR *ir);

/// Frees the index writer and waits for any merging threads to complete.
///
/// Arguments:
/// - `iw`: Pointer to the index writer.
void tantivy_writer_free(TantivySearchIndexW *iw);

} // extern "C"

#endif // TANTIVY_SEARCH_H
