#include <cstddef>
#include <cstdint>
#include <exception>
#include <new>
#include <string>
#include <utility>

namespace rust {
inline namespace cxxbridge1 {
// #include "rust/cxx.h"

namespace {
template <typename T>
class impl;
} // namespace

#ifndef CXXBRIDGE1_RUST_ERROR
#define CXXBRIDGE1_RUST_ERROR
class Error final : public std::exception {
public:
  Error(const Error &);
  Error(Error &&) noexcept;
  ~Error() noexcept override;

  Error &operator=(const Error &) &;
  Error &operator=(Error &&) &noexcept;

  const char *what() const noexcept override;

private:
  Error() noexcept = default;
  friend impl<Error>;
  const char *msg;
  std::size_t len;
};
#endif // CXXBRIDGE1_RUST_ERROR

namespace repr {
struct PtrLen final {
  void *ptr;
  ::std::size_t len;
};
} // namespace repr

namespace detail {
template <typename T, typename = void *>
struct operator_new {
  void *operator()(::std::size_t sz) { return ::operator new(sz); }
};

template <typename T>
struct operator_new<T, decltype(T::operator new(sizeof(T)))> {
  void *operator()(::std::size_t sz) { return T::operator new(sz); }
};
} // namespace detail

template <typename T>
union MaybeUninit {
  T value;
  void *operator new(::std::size_t sz) { return detail::operator_new<T>{}(sz); }
  MaybeUninit() {}
  ~MaybeUninit() {}
};

namespace {
template <>
class impl<Error> final {
public:
  static Error error(repr::PtrLen repr) noexcept {
    Error error;
    error.msg = static_cast<char const *>(repr.ptr);
    error.len = repr.len;
    return error;
  }
};
} // namespace
} // namespace cxxbridge1
} // namespace rust

extern "C" {
::rust::repr::PtrLen cxxbridge1$tantivy_create_index_with_tokenizer(::std::string const &index_path, ::std::string const &tokenizer_with_parameter, bool *return$) noexcept;

::rust::repr::PtrLen cxxbridge1$tantivy_create_index(::std::string const &index_path, bool *return$) noexcept;

::rust::repr::PtrLen cxxbridge1$tantivy_load_index(::std::string const &index_path, bool *return$) noexcept;

::rust::repr::PtrLen cxxbridge1$tantivy_index_doc(::std::string const &index_path, ::std::uint64_t row_id, ::std::string const &doc, bool *return$) noexcept;

::rust::repr::PtrLen cxxbridge1$tantivy_writer_commit(::std::string const &index_path, bool *return$) noexcept;

::rust::repr::PtrLen cxxbridge1$tantivy_reader_free(::std::string const &index_path, bool *return$) noexcept;

::rust::repr::PtrLen cxxbridge1$tantivy_writer_free(::std::string const &index_path, bool *return$) noexcept;

::rust::repr::PtrLen cxxbridge1$tantivy_search_in_rowid_range(::std::string const &index_path, ::std::string const &query, ::std::uint64_t lrange, ::std::uint64_t rrange, bool use_regex, bool *return$) noexcept;

::rust::repr::PtrLen cxxbridge1$tantivy_count_in_rowid_range(::std::string const &index_path, ::std::string const &query, ::std::uint64_t lrange, ::std::uint64_t rrange, bool use_regex, ::std::uint64_t *return$) noexcept;
} // extern "C"

bool tantivy_create_index_with_tokenizer(::std::string const &index_path, ::std::string const &tokenizer_with_parameter) {
  ::rust::MaybeUninit<bool> return$;
  ::rust::repr::PtrLen error$ = cxxbridge1$tantivy_create_index_with_tokenizer(index_path, tokenizer_with_parameter, &return$.value);
  if (error$.ptr) {
    throw ::rust::impl<::rust::Error>::error(error$);
  }
  return ::std::move(return$.value);
}

bool tantivy_create_index(::std::string const &index_path) {
  ::rust::MaybeUninit<bool> return$;
  ::rust::repr::PtrLen error$ = cxxbridge1$tantivy_create_index(index_path, &return$.value);
  if (error$.ptr) {
    throw ::rust::impl<::rust::Error>::error(error$);
  }
  return ::std::move(return$.value);
}

bool tantivy_load_index(::std::string const &index_path) {
  ::rust::MaybeUninit<bool> return$;
  ::rust::repr::PtrLen error$ = cxxbridge1$tantivy_load_index(index_path, &return$.value);
  if (error$.ptr) {
    throw ::rust::impl<::rust::Error>::error(error$);
  }
  return ::std::move(return$.value);
}

bool tantivy_index_doc(::std::string const &index_path, ::std::uint64_t row_id, ::std::string const &doc) {
  ::rust::MaybeUninit<bool> return$;
  ::rust::repr::PtrLen error$ = cxxbridge1$tantivy_index_doc(index_path, row_id, doc, &return$.value);
  if (error$.ptr) {
    throw ::rust::impl<::rust::Error>::error(error$);
  }
  return ::std::move(return$.value);
}

bool tantivy_writer_commit(::std::string const &index_path) {
  ::rust::MaybeUninit<bool> return$;
  ::rust::repr::PtrLen error$ = cxxbridge1$tantivy_writer_commit(index_path, &return$.value);
  if (error$.ptr) {
    throw ::rust::impl<::rust::Error>::error(error$);
  }
  return ::std::move(return$.value);
}

bool tantivy_reader_free(::std::string const &index_path) {
  ::rust::MaybeUninit<bool> return$;
  ::rust::repr::PtrLen error$ = cxxbridge1$tantivy_reader_free(index_path, &return$.value);
  if (error$.ptr) {
    throw ::rust::impl<::rust::Error>::error(error$);
  }
  return ::std::move(return$.value);
}

bool tantivy_writer_free(::std::string const &index_path) {
  ::rust::MaybeUninit<bool> return$;
  ::rust::repr::PtrLen error$ = cxxbridge1$tantivy_writer_free(index_path, &return$.value);
  if (error$.ptr) {
    throw ::rust::impl<::rust::Error>::error(error$);
  }
  return ::std::move(return$.value);
}

bool tantivy_search_in_rowid_range(::std::string const &index_path, ::std::string const &query, ::std::uint64_t lrange, ::std::uint64_t rrange, bool use_regex) {
  ::rust::MaybeUninit<bool> return$;
  ::rust::repr::PtrLen error$ = cxxbridge1$tantivy_search_in_rowid_range(index_path, query, lrange, rrange, use_regex, &return$.value);
  if (error$.ptr) {
    throw ::rust::impl<::rust::Error>::error(error$);
  }
  return ::std::move(return$.value);
}

::std::uint64_t tantivy_count_in_rowid_range(::std::string const &index_path, ::std::string const &query, ::std::uint64_t lrange, ::std::uint64_t rrange, bool use_regex) {
  ::rust::MaybeUninit<::std::uint64_t> return$;
  ::rust::repr::PtrLen error$ = cxxbridge1$tantivy_count_in_rowid_range(index_path, query, lrange, rrange, use_regex, &return$.value);
  if (error$.ptr) {
    throw ::rust::impl<::rust::Error>::error(error$);
  }
  return ::std::move(return$.value);
}