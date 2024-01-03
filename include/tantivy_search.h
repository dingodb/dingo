// SPDX-License-Identifier: Apache-2.0

#ifndef TANTIVY_SEARCH_H
#define TANTIVY_SEARCH_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

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

} // extern "C"

#endif // TANTIVY_SEARCH_H
