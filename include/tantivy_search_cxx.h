#pragma once
#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <iterator>
#include <new>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace rust {
inline namespace cxxbridge1 {
// #include "rust/cxx.h"

#ifndef CXXBRIDGE1_PANIC
#define CXXBRIDGE1_PANIC
template <typename Exception>
void panic [[noreturn]] (const char *msg);
#endif // CXXBRIDGE1_PANIC

struct unsafe_bitcopy_t;

namespace {
template <typename T>
class impl;
} // namespace

class Opaque;

template <typename T>
::std::size_t size_of();
template <typename T>
::std::size_t align_of();

#ifndef CXXBRIDGE1_RUST_STRING
#define CXXBRIDGE1_RUST_STRING
class String final {
public:
  String() noexcept;
  String(const String &) noexcept;
  String(String &&) noexcept;
  ~String() noexcept;

  String(const std::string &);
  String(const char *);
  String(const char *, std::size_t);
  String(const char16_t *);
  String(const char16_t *, std::size_t);

  static String lossy(const std::string &) noexcept;
  static String lossy(const char *) noexcept;
  static String lossy(const char *, std::size_t) noexcept;
  static String lossy(const char16_t *) noexcept;
  static String lossy(const char16_t *, std::size_t) noexcept;

  String &operator=(const String &) &noexcept;
  String &operator=(String &&) &noexcept;

  explicit operator std::string() const;

  const char *data() const noexcept;
  std::size_t size() const noexcept;
  std::size_t length() const noexcept;
  bool empty() const noexcept;

  const char *c_str() noexcept;

  std::size_t capacity() const noexcept;
  void reserve(size_t new_cap) noexcept;

  using iterator = char *;
  iterator begin() noexcept;
  iterator end() noexcept;

  using const_iterator = const char *;
  const_iterator begin() const noexcept;
  const_iterator end() const noexcept;
  const_iterator cbegin() const noexcept;
  const_iterator cend() const noexcept;

  bool operator==(const String &) const noexcept;
  bool operator!=(const String &) const noexcept;
  bool operator<(const String &) const noexcept;
  bool operator<=(const String &) const noexcept;
  bool operator>(const String &) const noexcept;
  bool operator>=(const String &) const noexcept;

  void swap(String &) noexcept;

  String(unsafe_bitcopy_t, const String &) noexcept;

private:
  struct lossy_t;
  String(lossy_t, const char *, std::size_t) noexcept;
  String(lossy_t, const char16_t *, std::size_t) noexcept;
  friend void swap(String &lhs, String &rhs) noexcept { lhs.swap(rhs); }

  std::array<std::uintptr_t, 3> repr;
};
#endif // CXXBRIDGE1_RUST_STRING

#ifndef CXXBRIDGE1_RUST_SLICE
#define CXXBRIDGE1_RUST_SLICE
namespace detail {
template <bool>
struct copy_assignable_if {};

template <>
struct copy_assignable_if<false> {
  copy_assignable_if() noexcept = default;
  copy_assignable_if(const copy_assignable_if &) noexcept = default;
  copy_assignable_if &operator=(const copy_assignable_if &) &noexcept = delete;
  copy_assignable_if &operator=(copy_assignable_if &&) &noexcept = default;
};
} // namespace detail

template <typename T>
class Slice final
    : private detail::copy_assignable_if<std::is_const<T>::value> {
public:
  using value_type = T;

  Slice() noexcept;
  Slice(T *, std::size_t count) noexcept;

  Slice &operator=(const Slice<T> &) &noexcept = default;
  Slice &operator=(Slice<T> &&) &noexcept = default;

  T *data() const noexcept;
  std::size_t size() const noexcept;
  std::size_t length() const noexcept;
  bool empty() const noexcept;

  T &operator[](std::size_t n) const noexcept;
  T &at(std::size_t n) const;
  T &front() const noexcept;
  T &back() const noexcept;

  Slice(const Slice<T> &) noexcept = default;
  ~Slice() noexcept = default;

  class iterator;
  iterator begin() const noexcept;
  iterator end() const noexcept;

  void swap(Slice &) noexcept;

private:
  class uninit;
  Slice(uninit) noexcept;
  friend impl<Slice>;
  friend void sliceInit(void *, const void *, std::size_t) noexcept;
  friend void *slicePtr(const void *) noexcept;
  friend std::size_t sliceLen(const void *) noexcept;

  std::array<std::uintptr_t, 2> repr;
};

template <typename T>
class Slice<T>::iterator final {
public:
  using iterator_category = std::random_access_iterator_tag;
  using value_type = T;
  using difference_type = std::ptrdiff_t;
  using pointer = typename std::add_pointer<T>::type;
  using reference = typename std::add_lvalue_reference<T>::type;

  reference operator*() const noexcept;
  pointer operator->() const noexcept;
  reference operator[](difference_type) const noexcept;

  iterator &operator++() noexcept;
  iterator operator++(int) noexcept;
  iterator &operator--() noexcept;
  iterator operator--(int) noexcept;

  iterator &operator+=(difference_type) noexcept;
  iterator &operator-=(difference_type) noexcept;
  iterator operator+(difference_type) const noexcept;
  iterator operator-(difference_type) const noexcept;
  difference_type operator-(const iterator &) const noexcept;

  bool operator==(const iterator &) const noexcept;
  bool operator!=(const iterator &) const noexcept;
  bool operator<(const iterator &) const noexcept;
  bool operator<=(const iterator &) const noexcept;
  bool operator>(const iterator &) const noexcept;
  bool operator>=(const iterator &) const noexcept;

private:
  friend class Slice;
  void *pos;
  std::size_t stride;
};

template <typename T>
Slice<T>::Slice() noexcept {
  sliceInit(this, reinterpret_cast<void *>(align_of<T>()), 0);
}

template <typename T>
Slice<T>::Slice(T *s, std::size_t count) noexcept {
  assert(s != nullptr || count == 0);
  sliceInit(this,
            s == nullptr && count == 0
                ? reinterpret_cast<void *>(align_of<T>())
                : const_cast<typename std::remove_const<T>::type *>(s),
            count);
}

template <typename T>
T *Slice<T>::data() const noexcept {
  return reinterpret_cast<T *>(slicePtr(this));
}

template <typename T>
std::size_t Slice<T>::size() const noexcept {
  return sliceLen(this);
}

template <typename T>
std::size_t Slice<T>::length() const noexcept {
  return this->size();
}

template <typename T>
bool Slice<T>::empty() const noexcept {
  return this->size() == 0;
}

template <typename T>
T &Slice<T>::operator[](std::size_t n) const noexcept {
  assert(n < this->size());
  auto ptr = static_cast<char *>(slicePtr(this)) + size_of<T>() * n;
  return *reinterpret_cast<T *>(ptr);
}

template <typename T>
T &Slice<T>::at(std::size_t n) const {
  if (n >= this->size()) {
    panic<std::out_of_range>("rust::Slice index out of range");
  }
  return (*this)[n];
}

template <typename T>
T &Slice<T>::front() const noexcept {
  assert(!this->empty());
  return (*this)[0];
}

template <typename T>
T &Slice<T>::back() const noexcept {
  assert(!this->empty());
  return (*this)[this->size() - 1];
}

template <typename T>
typename Slice<T>::iterator::reference
Slice<T>::iterator::operator*() const noexcept {
  return *static_cast<T *>(this->pos);
}

template <typename T>
typename Slice<T>::iterator::pointer
Slice<T>::iterator::operator->() const noexcept {
  return static_cast<T *>(this->pos);
}

template <typename T>
typename Slice<T>::iterator::reference Slice<T>::iterator::operator[](
    typename Slice<T>::iterator::difference_type n) const noexcept {
  auto ptr = static_cast<char *>(this->pos) + this->stride * n;
  return *reinterpret_cast<T *>(ptr);
}

template <typename T>
typename Slice<T>::iterator &Slice<T>::iterator::operator++() noexcept {
  this->pos = static_cast<char *>(this->pos) + this->stride;
  return *this;
}

template <typename T>
typename Slice<T>::iterator Slice<T>::iterator::operator++(int) noexcept {
  auto ret = iterator(*this);
  this->pos = static_cast<char *>(this->pos) + this->stride;
  return ret;
}

template <typename T>
typename Slice<T>::iterator &Slice<T>::iterator::operator--() noexcept {
  this->pos = static_cast<char *>(this->pos) - this->stride;
  return *this;
}

template <typename T>
typename Slice<T>::iterator Slice<T>::iterator::operator--(int) noexcept {
  auto ret = iterator(*this);
  this->pos = static_cast<char *>(this->pos) - this->stride;
  return ret;
}

template <typename T>
typename Slice<T>::iterator &Slice<T>::iterator::operator+=(
    typename Slice<T>::iterator::difference_type n) noexcept {
  this->pos = static_cast<char *>(this->pos) + this->stride * n;
  return *this;
}

template <typename T>
typename Slice<T>::iterator &Slice<T>::iterator::operator-=(
    typename Slice<T>::iterator::difference_type n) noexcept {
  this->pos = static_cast<char *>(this->pos) - this->stride * n;
  return *this;
}

template <typename T>
typename Slice<T>::iterator Slice<T>::iterator::operator+(
    typename Slice<T>::iterator::difference_type n) const noexcept {
  auto ret = iterator(*this);
  ret.pos = static_cast<char *>(this->pos) + this->stride * n;
  return ret;
}

template <typename T>
typename Slice<T>::iterator Slice<T>::iterator::operator-(
    typename Slice<T>::iterator::difference_type n) const noexcept {
  auto ret = iterator(*this);
  ret.pos = static_cast<char *>(this->pos) - this->stride * n;
  return ret;
}

template <typename T>
typename Slice<T>::iterator::difference_type
Slice<T>::iterator::operator-(const iterator &other) const noexcept {
  auto diff = std::distance(static_cast<char *>(other.pos),
                            static_cast<char *>(this->pos));
  return diff / static_cast<typename Slice<T>::iterator::difference_type>(
                    this->stride);
}

template <typename T>
bool Slice<T>::iterator::operator==(const iterator &other) const noexcept {
  return this->pos == other.pos;
}

template <typename T>
bool Slice<T>::iterator::operator!=(const iterator &other) const noexcept {
  return this->pos != other.pos;
}

template <typename T>
bool Slice<T>::iterator::operator<(const iterator &other) const noexcept {
  return this->pos < other.pos;
}

template <typename T>
bool Slice<T>::iterator::operator<=(const iterator &other) const noexcept {
  return this->pos <= other.pos;
}

template <typename T>
bool Slice<T>::iterator::operator>(const iterator &other) const noexcept {
  return this->pos > other.pos;
}

template <typename T>
bool Slice<T>::iterator::operator>=(const iterator &other) const noexcept {
  return this->pos >= other.pos;
}

template <typename T>
typename Slice<T>::iterator Slice<T>::begin() const noexcept {
  iterator it;
  it.pos = slicePtr(this);
  it.stride = size_of<T>();
  return it;
}

template <typename T>
typename Slice<T>::iterator Slice<T>::end() const noexcept {
  iterator it = this->begin();
  it.pos = static_cast<char *>(it.pos) + it.stride * this->size();
  return it;
}

template <typename T>
void Slice<T>::swap(Slice &rhs) noexcept {
  std::swap(*this, rhs);
}
#endif // CXXBRIDGE1_RUST_SLICE

#ifndef CXXBRIDGE1_RUST_BITCOPY_T
#define CXXBRIDGE1_RUST_BITCOPY_T
struct unsafe_bitcopy_t final {
  explicit unsafe_bitcopy_t() = default;
};
#endif // CXXBRIDGE1_RUST_BITCOPY_T

#ifndef CXXBRIDGE1_RUST_VEC
#define CXXBRIDGE1_RUST_VEC
template <typename T>
class Vec final {
public:
  using value_type = T;

  Vec() noexcept;
  Vec(std::initializer_list<T>);
  Vec(const Vec &);
  Vec(Vec &&) noexcept;
  ~Vec() noexcept;

  Vec &operator=(Vec &&) &noexcept;
  Vec &operator=(const Vec &) &;

  std::size_t size() const noexcept;
  bool empty() const noexcept;
  const T *data() const noexcept;
  T *data() noexcept;
  std::size_t capacity() const noexcept;

  const T &operator[](std::size_t n) const noexcept;
  const T &at(std::size_t n) const;
  const T &front() const noexcept;
  const T &back() const noexcept;

  T &operator[](std::size_t n) noexcept;
  T &at(std::size_t n);
  T &front() noexcept;
  T &back() noexcept;

  void reserve(std::size_t new_cap);
  void push_back(const T &value);
  void push_back(T &&value);
  template <typename... Args>
  void emplace_back(Args &&...args);
  void truncate(std::size_t len);
  void clear();

  using iterator = typename Slice<T>::iterator;
  iterator begin() noexcept;
  iterator end() noexcept;

  using const_iterator = typename Slice<const T>::iterator;
  const_iterator begin() const noexcept;
  const_iterator end() const noexcept;
  const_iterator cbegin() const noexcept;
  const_iterator cend() const noexcept;

  void swap(Vec &) noexcept;

  Vec(unsafe_bitcopy_t, const Vec &) noexcept;

private:
  void reserve_total(std::size_t new_cap) noexcept;
  void set_len(std::size_t len) noexcept;
  void drop() noexcept;

  friend void swap(Vec &lhs, Vec &rhs) noexcept { lhs.swap(rhs); }

  std::array<std::uintptr_t, 3> repr;
};

template <typename T>
Vec<T>::Vec(std::initializer_list<T> init) : Vec{} {
  this->reserve_total(init.size());
  std::move(init.begin(), init.end(), std::back_inserter(*this));
}

template <typename T>
Vec<T>::Vec(const Vec &other) : Vec() {
  this->reserve_total(other.size());
  std::copy(other.begin(), other.end(), std::back_inserter(*this));
}

template <typename T>
Vec<T>::Vec(Vec &&other) noexcept : repr(other.repr) {
  new (&other) Vec();
}

template <typename T>
Vec<T>::~Vec() noexcept {
  this->drop();
}

template <typename T>
Vec<T> &Vec<T>::operator=(Vec &&other) &noexcept {
  this->drop();
  this->repr = other.repr;
  new (&other) Vec();
  return *this;
}

template <typename T>
Vec<T> &Vec<T>::operator=(const Vec &other) & {
  if (this != &other) {
    this->drop();
    new (this) Vec(other);
  }
  return *this;
}

template <typename T>
bool Vec<T>::empty() const noexcept {
  return this->size() == 0;
}

template <typename T>
T *Vec<T>::data() noexcept {
  return const_cast<T *>(const_cast<const Vec<T> *>(this)->data());
}

template <typename T>
const T &Vec<T>::operator[](std::size_t n) const noexcept {
  assert(n < this->size());
  auto data = reinterpret_cast<const char *>(this->data());
  return *reinterpret_cast<const T *>(data + n * size_of<T>());
}

template <typename T>
const T &Vec<T>::at(std::size_t n) const {
  if (n >= this->size()) {
    panic<std::out_of_range>("rust::Vec index out of range");
  }
  return (*this)[n];
}

template <typename T>
const T &Vec<T>::front() const noexcept {
  assert(!this->empty());
  return (*this)[0];
}

template <typename T>
const T &Vec<T>::back() const noexcept {
  assert(!this->empty());
  return (*this)[this->size() - 1];
}

template <typename T>
T &Vec<T>::operator[](std::size_t n) noexcept {
  assert(n < this->size());
  auto data = reinterpret_cast<char *>(this->data());
  return *reinterpret_cast<T *>(data + n * size_of<T>());
}

template <typename T>
T &Vec<T>::at(std::size_t n) {
  if (n >= this->size()) {
    panic<std::out_of_range>("rust::Vec index out of range");
  }
  return (*this)[n];
}

template <typename T>
T &Vec<T>::front() noexcept {
  assert(!this->empty());
  return (*this)[0];
}

template <typename T>
T &Vec<T>::back() noexcept {
  assert(!this->empty());
  return (*this)[this->size() - 1];
}

template <typename T>
void Vec<T>::reserve(std::size_t new_cap) {
  this->reserve_total(new_cap);
}

template <typename T>
void Vec<T>::push_back(const T &value) {
  this->emplace_back(value);
}

template <typename T>
void Vec<T>::push_back(T &&value) {
  this->emplace_back(std::move(value));
}

template <typename T>
template <typename... Args>
void Vec<T>::emplace_back(Args &&...args) {
  auto size = this->size();
  this->reserve_total(size + 1);
  ::new (reinterpret_cast<T *>(reinterpret_cast<char *>(this->data()) +
                               size * size_of<T>()))
      T(std::forward<Args>(args)...);
  this->set_len(size + 1);
}

template <typename T>
void Vec<T>::clear() {
  this->truncate(0);
}

template <typename T>
typename Vec<T>::iterator Vec<T>::begin() noexcept {
  return Slice<T>(this->data(), this->size()).begin();
}

template <typename T>
typename Vec<T>::iterator Vec<T>::end() noexcept {
  return Slice<T>(this->data(), this->size()).end();
}

template <typename T>
typename Vec<T>::const_iterator Vec<T>::begin() const noexcept {
  return this->cbegin();
}

template <typename T>
typename Vec<T>::const_iterator Vec<T>::end() const noexcept {
  return this->cend();
}

template <typename T>
typename Vec<T>::const_iterator Vec<T>::cbegin() const noexcept {
  return Slice<const T>(this->data(), this->size()).begin();
}

template <typename T>
typename Vec<T>::const_iterator Vec<T>::cend() const noexcept {
  return Slice<const T>(this->data(), this->size()).end();
}

template <typename T>
void Vec<T>::swap(Vec &rhs) noexcept {
  using std::swap;
  swap(this->repr, rhs.repr);
}

template <typename T>
Vec<T>::Vec(unsafe_bitcopy_t, const Vec &bits) noexcept : repr(bits.repr) {}
#endif // CXXBRIDGE1_RUST_VEC

#ifndef CXXBRIDGE1_IS_COMPLETE
#define CXXBRIDGE1_IS_COMPLETE
namespace detail {
namespace {
template <typename T, typename = std::size_t>
struct is_complete : std::false_type {};
template <typename T>
struct is_complete<T, decltype(sizeof(T))> : std::true_type {};
} // namespace
} // namespace detail
#endif // CXXBRIDGE1_IS_COMPLETE

#ifndef CXXBRIDGE1_LAYOUT
#define CXXBRIDGE1_LAYOUT
class layout {
  template <typename T>
  friend std::size_t size_of();
  template <typename T>
  friend std::size_t align_of();
  template <typename T>
  static typename std::enable_if<std::is_base_of<Opaque, T>::value,
                                 std::size_t>::type
  do_size_of() {
    return T::layout::size();
  }
  template <typename T>
  static typename std::enable_if<!std::is_base_of<Opaque, T>::value,
                                 std::size_t>::type
  do_size_of() {
    return sizeof(T);
  }
  template <typename T>
  static
      typename std::enable_if<detail::is_complete<T>::value, std::size_t>::type
      size_of() {
    return do_size_of<T>();
  }
  template <typename T>
  static typename std::enable_if<std::is_base_of<Opaque, T>::value,
                                 std::size_t>::type
  do_align_of() {
    return T::layout::align();
  }
  template <typename T>
  static typename std::enable_if<!std::is_base_of<Opaque, T>::value,
                                 std::size_t>::type
  do_align_of() {
    return alignof(T);
  }
  template <typename T>
  static
      typename std::enable_if<detail::is_complete<T>::value, std::size_t>::type
      align_of() {
    return do_align_of<T>();
  }
};

template <typename T>
std::size_t size_of() {
  return layout::size_of<T>();
}

template <typename T>
std::size_t align_of() {
  return layout::align_of<T>();
}
#endif // CXXBRIDGE1_LAYOUT
} // namespace cxxbridge1
} // namespace rust

struct RowIdWithScore;
struct DocWithFreq;
struct FieldTokenNums;
struct Statistics;

#ifndef CXXBRIDGE1_STRUCT_RowIdWithScore
#define CXXBRIDGE1_STRUCT_RowIdWithScore
struct RowIdWithScore final {
  ::std::uint64_t row_id;
  float score;
  ::std::uint32_t seg_id;
  ::std::uint32_t doc_id;
  ::rust::Vec<::rust::String> docs;

  using IsRelocatable = ::std::true_type;
};
#endif // CXXBRIDGE1_STRUCT_RowIdWithScore

#ifndef CXXBRIDGE1_STRUCT_DocWithFreq
#define CXXBRIDGE1_STRUCT_DocWithFreq
struct DocWithFreq final {
  ::rust::String term_str;
  ::std::uint32_t field_id;
  ::std::uint64_t doc_freq;

  bool operator==(DocWithFreq const &) const noexcept;
  bool operator!=(DocWithFreq const &) const noexcept;
  using IsRelocatable = ::std::true_type;
};
#endif // CXXBRIDGE1_STRUCT_DocWithFreq

#ifndef CXXBRIDGE1_STRUCT_FieldTokenNums
#define CXXBRIDGE1_STRUCT_FieldTokenNums
struct FieldTokenNums final {
  ::std::uint32_t field_id;
  ::std::uint64_t field_total_tokens;

  bool operator==(FieldTokenNums const &) const noexcept;
  bool operator!=(FieldTokenNums const &) const noexcept;
  using IsRelocatable = ::std::true_type;
};
#endif // CXXBRIDGE1_STRUCT_FieldTokenNums

#ifndef CXXBRIDGE1_STRUCT_Statistics
#define CXXBRIDGE1_STRUCT_Statistics
struct Statistics final {
  ::rust::Vec<::DocWithFreq> docs_freq;
  ::std::uint64_t total_num_tokens;
  ::std::uint64_t total_num_docs;

  using IsRelocatable = ::std::true_type;
};
#endif // CXXBRIDGE1_STRUCT_Statistics

bool ffi_varify_index_parameter(::std::string const &index_json_parameter) noexcept;

// Create tantivy index.
// arguments:
// - `index_path`: index directory.
// - `column_names`: which columns will be used to build index.
// - `index_json_parameter`: config index with json.
bool ffi_create_index_with_parameter(::std::string const &index_path, ::std::vector<::std::string> const &column_names, ::std::string const &index_json_parameter) noexcept;

// Create tantivy index by default.
// argements:
// - `index_path`: index directory.
// - `column_names`: which columns will be used to build index.
bool ffi_create_index(::std::string const &index_path, ::std::vector<::std::string> const &column_names) noexcept;

// Index multi column docs with given rowId.
// arguments:
// - `index_path`: index directory.
// - `row_id`: row_id given by ClickHouse.
// - `column_names`: align with column_docs.
// - `column_docs`: align with column_names.
bool ffi_index_multi_column_docs(::std::string const &index_path, ::std::uint64_t row_id, ::std::vector<::std::string> const &column_names, ::std::vector<::std::string> const &column_docs) noexcept;

// Delete a group of rowIds.
// arguments:
// - `index_path`: index directory.
// - `row_ids`: a group of rowIds need be deleted.
bool ffi_delete_row_ids(::std::string const &index_path, ::std::vector<::std::uint32_t> const &row_ids) noexcept;

// Commit index writer
// arguments:
// - `index_path`: index directory.
bool ffi_index_writer_commit(::std::string const &index_path) noexcept;

// Free index writer
// arguments:
// - `index_path`: index directory.
bool ffi_free_index_writer(::std::string const &index_path) noexcept;

// Load index reader
// arguments:
// - `index_path`: index directory.
bool ffi_load_index_reader(::std::string const &index_path) noexcept;

// Free index reader
// arguments:
// - `index_path`: index directory.
bool ffi_free_index_reader(::std::string const &index_path) noexcept;

// Get indexed docs numbers.
// arguments:
// - `index_path`: index directory.
::std::uint64_t ffi_get_indexed_doc_counts(::std::string const &index_path) noexcept;

// Execute single term query with given rowId range.
// arguments:
// - `index_path`: index directory.
// - `column_name`: which column will execute search.
// - `term`: term needs to be searched.
// - `lrange`: rowId left begin.
// - `rrange`: rowId right end.
bool ffi_query_term_with_range(::std::string const &index_path, ::std::string const &column_name, ::std::string const &term, ::std::uint64_t lrange, ::std::uint64_t rrange) noexcept;

// Execute a group of terms query with given rowId range.
// arguments:
// - `index_path`: index directory.
// - `column_name`: which column will execute search.
// - `terms`: a group of terms need to be searched.
// - `lrange`: rowId left begin.
// - `rrange`: rowId right end.
bool ffi_query_terms_with_range(::std::string const &index_path, ::std::string const &column_name, ::std::vector<::std::string> const &terms, ::std::uint64_t lrange, ::std::uint64_t rrange) noexcept;

// Execute a sentence query with given rowId range.
// arguments:
// - `index_path`: index directory.
// - `column_name`: which column will execute search.
// - `sentence`: sentence needs to be searched.
// - `lrange`: rowId left begin.
// - `rrange`: rowId right end.
bool ffi_query_sentence_with_range(::std::string const &index_path, ::std::string const &column_name, ::std::string const &sentence, ::std::uint64_t lrange, ::std::uint64_t rrange) noexcept;

// Execute a regex query with given rowId range.
// arguments:
// - `index_path`: index directory.
// - `column_name`: which column will execute search.
// - `pattern`: pattern should be given by ClickHouse.
// - `lrange`: rowId left begin.
// - `rrange`: rowId right end.
bool ffi_regex_term_with_range(::std::string const &index_path, ::std::string const &column_name, ::std::string const &pattern, ::std::uint64_t lrange, ::std::uint64_t rrange) noexcept;

// Execute a term query and return rowIds u8 bitmap.
// arguments:
// - `index_path`: index directory.
// - `column_name`: which column will execute search.
// - `term`: term needs to be searched.
::rust::Vec<::std::uint8_t> ffi_query_term_bitmap(::std::string const &index_path, ::std::string const &column_name, ::std::string const &term) noexcept;

// Execute a group of terms query and return rowIds u8 bitmap.
// arguments:
// - `index_path`: index directory.
// - `column_name`: which column will execute search.
// - `terms`: terms need to be searched.
::rust::Vec<::std::uint8_t> ffi_query_terms_bitmap(::std::string const &index_path, ::std::string const &column_name, ::std::vector<::std::string> const &terms) noexcept;

// Execute a sentence query and return rowIds u8 bitmap.
// arguments:
// - `index_path`: index directory.
// - `column_name`: which column will execute search.
// - `sentence`: sentence needs to be searched.
::rust::Vec<::std::uint8_t> ffi_query_sentence_bitmap(::std::string const &index_path, ::std::string const &column_name, ::std::string const &sentence) noexcept;

// Execute a regex query and return rowIds u8 bitmap.
// arguments:
// - `index_path`: index directory.
// - `column_name`: which column will execute search.
// - `pattern`: pattern should be given by ClickHouse.
::rust::Vec<::std::uint8_t> ffi_regex_term_bitmap(::std::string const &index_path, ::std::string const &column_name, ::std::string const &pattern) noexcept;

// Execute a regex query and return rowIds u8 bitmap.
// arguments:
// - `index_path`: index directory.
// - `sentence`: from ClickHouse TextSearch function.
// - `topk`: only return top k related results.
// - `u8_aived_bitmap`: alived rowIds given by u8 bitmap.
// - `query_with_filter`: whether use alived_bitmap or not.
// - `statistics`: for multi parts bm25 statistics info.
::rust::Vec<::RowIdWithScore> ffi_bm25_search_with_stat(::std::string const &index_path, ::std::string const &sentence, ::std::uint32_t topk, ::std::vector<::std::uint8_t> const &u8_aived_bitmap, bool query_with_filter, ::Statistics const &statistics) noexcept;

// Execute a regex query and return rowIds u8 bitmap.
// arguments:
// - `index_path`: index directory.
// - `sentence`: from ClickHouse TextSearch function.
// - `topk`: only return top k related results.
// - `u8_aived_bitmap`: alived rowIds given by u8 bitmap.
// - `query_with_filter`: whether use alived_bitmap or not.
// - `statistics`: for multi parts bm25 statistics info.
::rust::Vec<::RowIdWithScore> ffi_bm25_search(::std::string const &index_path, ::std::string const &sentence, ::std::uint32_t topk, ::std::vector<::std::uint8_t> const &u8_aived_bitmap, bool query_with_filter) noexcept;

// Get doc freq for current part.
// arguments:
// - `index_path`: index directory.
// - `sentence`: query_str.
::rust::Vec<::DocWithFreq> ffi_get_doc_freq(::std::string const &index_path, ::std::string const &sentence) noexcept;

// Get total num docs for current part.
// arguments:
// - `index_path`: index directory.
::std::uint64_t ffi_get_total_num_docs(::std::string const &index_path) noexcept;

// Get total num tokens for current part.
// arguments:
// - `index_path`: index directory.
::std::uint64_t ffi_get_total_num_tokens(::std::string const &index_path) noexcept;
