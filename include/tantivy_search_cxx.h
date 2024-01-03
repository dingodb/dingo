#pragma once
#include <cstdint>
#include <string>

namespace TANTIVY {
bool tantivy_create_index_with_tokenizer(::std::string const &index_path, ::std::string const &tokenizer_with_parameter);

bool tantivy_create_index(::std::string const &index_path);

bool tantivy_load_index(::std::string const &index_path);

bool tantivy_index_doc(::std::string const &index_path, ::std::uint64_t row_id, ::std::string const &doc);

bool tantivy_writer_commit(::std::string const &index_path);

bool tantivy_reader_free(::std::string const &index_path);

bool tantivy_writer_free(::std::string const &index_path);

bool tantivy_search_in_rowid_range(::std::string const &index_path, ::std::string const &query, ::std::uint64_t lrange, ::std::uint64_t rrange, bool use_regex);

::std::uint64_t tantivy_count_in_rowid_range(::std::string const &index_path, ::std::string const &query, ::std::uint64_t lrange, ::std::uint64_t rrange, bool use_regex);
} // namespace TANTIVY
