// SPDX-License-Identifier: Apache-2.0

#ifndef TANTIVY_SEARCH_H
#define TANTIVY_SEARCH_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

using TantivySearchLogCallback = void(*)(int32_t, const char*, const char*);

extern "C" {

bool tantivy_search_log4rs_initialize(const char *log_directory,
                                      const char *log_level,
                                      bool log_in_file,
                                      bool console_dispaly,
                                      bool only_record_tantivy_search);

/// Initializes the logger configuration for the tantivy search library.
///
/// Arguments:
/// - `log_path`: The path where log files are saved. Tantivy-search will generate multiple log files.
/// - `log_level`: The logging level to use. Supported levels: info, debug, trace, error, warning.
/// - `log_in_file`: Whether record log content in file.
///   Note: 'fatal' is treated as 'error'.
/// - `console_dispaly`: Enables logging to the console if set to true.
/// - `only_tantivy_search`: Only record `target=tantivy_search` log content.
/// - `callback`: A callback function, typically provided by ClickHouse.
///
/// Returns:
/// - `true` if the logger is successfully initialized, `false` otherwise.
bool tantivy_search_log4rs_initialize_with_callback(const char *log_directory,
                                                    const char *log_level,
                                                    bool log_in_file,
                                                    bool console_dispaly,
                                                    bool only_record_tantivy_search,
                                                    TantivySearchLogCallback callback);

} // extern "C"

#endif // TANTIVY_SEARCH_H
