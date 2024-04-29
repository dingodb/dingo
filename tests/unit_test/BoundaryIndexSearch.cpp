#include <gtest/gtest.h>
#include <tantivy_search.h>
#include <utils.h>

#include <filesystem>
#include <iostream>
#include <vector>

namespace fs = std::filesystem;
using namespace Utils;
using namespace std;

#define FFI_ASSERT_TRUE(condition) ASSERT_TRUE(condition.result)
#define FFI_ASSERT_FALSE(condition) ASSERT_FALSE(condition.result)

class BoundaryTantivySearchReaderLoadTest : public ::testing::Test {
 protected:
  const string logPath = "./log";
  const string indexDirectory = "./temp";
  const string indexDirectoryNotExists = "./temp2";
  const vector<string> column_names = {"col1", "col2", "col3"};
  void SetUp() {
    ASSERT_TRUE(tantivy_search_log4rs_initialize(logPath.c_str(), "info", true, false, true));
    FFI_ASSERT_TRUE(ffi_create_index(indexDirectory, column_names));
  }
  void TearDown() {
    FFI_ASSERT_TRUE(ffi_free_index_writer(indexDirectory));
    fs::remove_all(indexDirectory);
    fs::remove_all(indexDirectoryNotExists);
  }
};

TEST_F(BoundaryTantivySearchReaderLoadTest, readerLoadAndFreeWithExistsIndex) {
  for (size_t i = 0; i < 100; i++) {
    FFI_ASSERT_TRUE(ffi_load_index_reader(indexDirectory));
    FFI_ASSERT_TRUE(ffi_load_index_reader(indexDirectory));

    FFI_ASSERT_TRUE(ffi_free_index_reader(indexDirectory));
    FFI_ASSERT_FALSE(ffi_free_index_reader(indexDirectory));
  }
}

TEST_F(BoundaryTantivySearchReaderLoadTest, readerLoadAndFreeWithoutIndex) {
  FFI_ASSERT_FALSE(ffi_load_index_reader(indexDirectoryNotExists));
  FFI_ASSERT_FALSE(ffi_free_index_reader(indexDirectoryNotExists));
}

TEST_F(BoundaryTantivySearchReaderLoadTest, nullptrParameter) {
  ASSERT_ANY_THROW(ffi_load_index_reader(nullptr));
  ASSERT_ANY_THROW(ffi_free_index_reader(nullptr));
}

class BoundaryFFiSearchTest : public ::testing::Test, public BoundaryUnitTestUtils {
 protected:
  const string logPath = "./log";
  const string indexDirectory = "./temp";
  const string indexDirectoryNotExists = "./temp2";
  const string indexEmptyDirectory = "./temp3";
  const vector<string> column_names = {"col1", "col2", "col3"};

  void SetUp() {
    ASSERT_TRUE(tantivy_search_log4rs_initialize(logPath.c_str(), "info", true, false, true));
    FFI_ASSERT_TRUE(ffi_create_index(indexEmptyDirectory, column_names));
    FFI_ASSERT_TRUE(ffi_load_index_reader(indexEmptyDirectory));
    FFI_ASSERT_TRUE(ffi_create_index(indexDirectory, column_names));
    // Index 2w docs, each doc length is 1k.
    // u32 range: 0 ~ 4294967295
    for (uint64_t i = 4294960000; i < 4294980000; i++) {
      FFI_ASSERT_TRUE(ffi_index_multi_column_docs(
          indexDirectory, i, column_names,
          {generateRandomString(1000), generateRandomString(1000), generateRandomString(1000)}));
    }
    FFI_ASSERT_TRUE(ffi_index_writer_commit(indexDirectory));
    FFI_ASSERT_TRUE(ffi_load_index_reader(indexDirectory));
  }
  void TearDown() {
    FFI_ASSERT_TRUE(ffi_free_index_writer(indexDirectory));
    FFI_ASSERT_TRUE(ffi_free_index_writer(indexEmptyDirectory));
    fs::remove_all(indexDirectory);
    fs::remove_all(indexDirectoryNotExists);
    fs::remove_all(indexEmptyDirectory);
  }
};

TEST_F(BoundaryFFiSearchTest, ffiQuerySentenceWithRangeU32NotOverflow) {
  for (uint32_t i = 4294960000; i < (4294960000 + 1000); i++) {
    ASSERT_NO_THROW(
        ffi_query_sentence_with_range(indexDirectory, column_names[0], generateRandomString(10), i - 1000, i + 1000));
    ASSERT_NO_THROW(
        ffi_query_sentence_with_range(indexDirectory, column_names[0], generateRandomString(200), i - 1000, i + 5000));
  }
}

TEST_F(BoundaryFFiSearchTest, ffiQuerySentenceWithInvalidRange) {
  FFI_ASSERT_FALSE(ffi_query_sentence_with_range(indexDirectory, column_names[0], generateRandomString(10), 100, 10));
}

TEST_F(BoundaryFFiSearchTest, ffiQuerySentenceWithU64OverflowRange) {
  for (uint64_t i = 4294968000; i < (4294968000 + 1000); i++) {
    FFI_ASSERT_FALSE(
        ffi_query_sentence_with_range(indexDirectory, column_names[0], generateRandomString(10), i, i + 1000));
    FFI_ASSERT_FALSE(
        ffi_query_sentence_with_range(indexDirectory, column_names[0], generateRandomString(200), i, i + 5000));
    FFI_ASSERT_FALSE(
        ffi_query_sentence_with_range(indexDirectory, column_names[0], generateRandomString(5000), i, i + 8000));
    FFI_ASSERT_FALSE(
        ffi_query_sentence_with_range(indexDirectory, column_names[0], generateRandomString(10000), i, i + 10000));
  }
}

TEST_F(BoundaryFFiSearchTest, ffiQuerySentenceWithRangeNoReader) {
  FFI_ASSERT_TRUE(ffi_free_index_reader(indexDirectory));
  FFI_ASSERT_FALSE(ffi_query_sentence_with_range(indexDirectory, column_names[0], generateRandomString(100), 4294960000,
                                                 4294960000 + 1000));
  FFI_ASSERT_FALSE(ffi_query_sentence_with_range(indexDirectory, column_names[0], generateRandomString(100), 4294960000,
                                                 4294960000 + 1000));
  FFI_ASSERT_TRUE(ffi_load_index_reader(indexDirectory));
  ASSERT_NO_THROW(ffi_query_sentence_with_range(indexDirectory, column_names[0], generateRandomString(100), 4294960000,
                                                4294960000 + 1000));
  ASSERT_NO_THROW(ffi_query_sentence_with_range(indexDirectory, column_names[0], generateRandomString(100), 4294960000,
                                                4294960000 + 1000));
  FFI_ASSERT_TRUE(ffi_free_index_reader(indexDirectory));
}

TEST_F(BoundaryFFiSearchTest, ffiQuerySentenceWithRangeNoIndex) {
  FFI_ASSERT_FALSE(ffi_query_sentence_with_range(indexDirectoryNotExists, column_names[0], generateRandomString(100),
                                                 4294960000, 4294960000 + 1000));
  FFI_ASSERT_FALSE(ffi_query_sentence_with_range(indexDirectoryNotExists, column_names[0], generateRandomString(100),
                                                 4294960000, 4294960000 + 1000));
}

TEST_F(BoundaryFFiSearchTest, ffiQuerySentenceWithRangeEmptyIndex) {
  FFI_ASSERT_FALSE(ffi_query_sentence_with_range(indexEmptyDirectory, column_names[0], generateRandomString(100),
                                                 4294960000, 4294960000 + 1000));
  FFI_ASSERT_FALSE(ffi_query_sentence_with_range(indexEmptyDirectory, column_names[0], generateRandomString(100),
                                                 4294960000, 4294960000 + 1000));
}

TEST_F(BoundaryFFiSearchTest, ffiQuerySentenceWithRangeNullptrParameter) {
  ASSERT_ANY_THROW(ffi_query_sentence_with_range(nullptr, column_names[0], generateRandomString(100), 0, 100));
  ASSERT_ANY_THROW(ffi_query_sentence_with_range(indexEmptyDirectory, column_names[0], nullptr, 0, 100));
  ASSERT_ANY_THROW(ffi_query_sentence_with_range(nullptr, nullptr, nullptr, 0, 100));
}

TEST_F(BoundaryFFiSearchTest, ffiBM25Search) {
  for (size_t i = 0; i < 100; i++) {
    ASSERT_NO_THROW(ffi_bm25_search(indexDirectory, generateRandomNormalString(i), 10, {}, false));
    ASSERT_NO_THROW(ffi_bm25_search(indexDirectory, generateRandomNormalString(i), 1000000, {}, false));
    ASSERT_NO_THROW(ffi_bm25_search(indexDirectory, generateRandomNormalString(i), 10, {}, false));
    ASSERT_NO_THROW(ffi_bm25_search(indexDirectory, generateRandomNormalString(i), 100000, {}, false));
    ASSERT_NO_THROW(ffi_bm25_search(indexDirectory, generateRandomNormalString(i), 100000, {}, true));
    ASSERT_NO_THROW(ffi_bm25_search(indexDirectory, generateRandomNormalString(i), 10, {255, 255}, true));
  }
}

TEST_F(BoundaryFFiSearchTest, ffiBM25SearchNoReader) {
  FFI_ASSERT_TRUE(ffi_free_index_reader(indexDirectory));

  ASSERT_TRUE(ffi_bm25_search(indexDirectory, generateRandomNormalString(100), 10, {}, false).result.size() == 0);
  ASSERT_TRUE(ffi_bm25_search(indexDirectory, generateRandomNormalString(100), 1000000, {}, false).result.size() == 0);
  ASSERT_TRUE(
      ffi_bm25_search(indexDirectory, generateRandomNormalString(100), 10, {255, 255, 255}, true).result.size() == 0);
  ASSERT_TRUE(
      ffi_bm25_search(indexDirectory, generateRandomNormalString(100), 100000, {255, 255, 255}, false).result.size() ==
      0);

  FFI_ASSERT_TRUE(ffi_load_index_reader(indexDirectory));

  ASSERT_FALSE(ffi_bm25_search(indexDirectory, generateRandomNormalString(100), 10, {}, false).result.size() == 0);
  ASSERT_FALSE(ffi_bm25_search(indexDirectory, generateRandomNormalString(100), 1000000, {}, false).result.size() == 0);
  ASSERT_FALSE(
      ffi_bm25_search(indexDirectory, generateRandomNormalString(100), 10, {255, 255, 255}, true).result.size() == 0);
  ASSERT_FALSE(
      ffi_bm25_search(indexDirectory, generateRandomNormalString(100), 100000, {255, 255, 255}, false).result.size() ==
      0);

  FFI_ASSERT_TRUE(ffi_free_index_reader(indexDirectory));
}

TEST_F(BoundaryFFiSearchTest, ffiBM25SearchNoIndex) {
  ASSERT_TRUE(ffi_bm25_search(indexDirectoryNotExists, generateRandomNormalString(100), 10, {}, false).result.size() ==
              0);
  ASSERT_TRUE(
      ffi_bm25_search(indexDirectoryNotExists, generateRandomNormalString(100), 1000000, {}, false).result.size() == 0);
  ASSERT_TRUE(ffi_bm25_search(indexDirectoryNotExists, generateRandomNormalString(100), 10, {255, 255, 255}, true)
                  .result.size() == 0);
  ASSERT_TRUE(ffi_bm25_search(indexDirectoryNotExists, generateRandomNormalString(100), 100000, {255, 255, 255}, false)
                  .result.size() == 0);
}

TEST_F(BoundaryFFiSearchTest, ffiBM25SearchEmptyIndex) {
  ASSERT_TRUE(ffi_bm25_search(indexEmptyDirectory, generateRandomNormalString(100), 10, {}, false).result.size() == 0);
  ASSERT_TRUE(ffi_bm25_search(indexEmptyDirectory, generateRandomNormalString(100), 1000000, {}, false).result.size() ==
              0);
  ASSERT_TRUE(
      ffi_bm25_search(indexEmptyDirectory, generateRandomNormalString(100), 10, {255, 255, 255}, true).result.size() ==
      0);
  ASSERT_TRUE(ffi_bm25_search(indexEmptyDirectory, generateRandomNormalString(100), 100000, {255, 255, 255}, false)
                  .result.size() == 0);
}

TEST_F(BoundaryFFiSearchTest, ffiBM25SearchWithFilter) {
  for (size_t i = 0; i < 100; i++) {
    ASSERT_NO_THROW(
        ffi_bm25_search(indexDirectory, generateRandomNormalString(i), 10, generateRandomUInt8Vector(i), true));
    ASSERT_NO_THROW(
        ffi_bm25_search(indexDirectory, generateRandomNormalString(i), 100000, generateRandomUInt8Vector(i), true));
  }
}

TEST_F(BoundaryFFiSearchTest, ffiBM25SearchNullptrParameter) {
  ASSERT_ANY_THROW(ffi_bm25_search(indexDirectory, nullptr, 10, {}, false));
  ASSERT_ANY_THROW(ffi_bm25_search(nullptr, generateRandomNormalString(10), 10, generateRandomUInt8Vector(10), true));
  ASSERT_ANY_THROW(ffi_bm25_search(nullptr, nullptr, 10, generateRandomUInt8Vector(10), true));
}

TEST_F(BoundaryFFiSearchTest, ffiQueryTermBitmap) {
  for (size_t i = 0; i < 100; i++) {
    ASSERT_NO_THROW(ffi_query_term_bitmap(indexDirectory, column_names[0], "ancient"));
    ASSERT_NO_THROW(ffi_query_sentence_bitmap(indexDirectory, column_names[0], generateRandomNormalString(i)));
  }
}

TEST_F(BoundaryFFiSearchTest, ffiQuerySentenceBitmapEmptyIndex) {
  for (size_t i = 0; i < 100; i++) {
    ASSERT_NO_THROW(ffi_query_sentence_bitmap(indexEmptyDirectory, column_names[1], generateRandomNormalString(i)));
  }
}

TEST_F(BoundaryFFiSearchTest, ffiQuerySentenceBitmapNoIndex) {
  for (size_t i = 0; i < 100; i++) {
    ASSERT_TRUE(
        ffi_query_sentence_bitmap(indexDirectoryNotExists, column_names[0], generateRandomNormalString(i)).size() == 0);
    ASSERT_TRUE(
        ffi_query_sentence_bitmap(indexDirectoryNotExists, column_names[1], generateRandomNormalString(i)).size() == 0);
    ASSERT_TRUE(
        ffi_query_sentence_bitmap(indexDirectoryNotExists, column_names[2], generateRandomNormalString(i)).size() == 0);
  }
}

TEST_F(BoundaryFFiSearchTest, ffiQuerySentenceBitmapNoReader) {
  FFI_ASSERT_TRUE(ffi_free_index_reader(indexDirectory));
  for (size_t i = 0; i < 100; i++) {
    ASSERT_TRUE(ffi_query_sentence_bitmap(indexDirectory, column_names[0], generateRandomNormalString(i)).size() == 0);
    ASSERT_TRUE(ffi_query_sentence_bitmap(indexDirectory, column_names[1], generateRandomNormalString(i)).size() == 0);
    ASSERT_TRUE(ffi_query_sentence_bitmap(indexDirectory, column_names[2], generateRandomNormalString(i)).size() == 0);
  }
  FFI_ASSERT_TRUE(ffi_load_index_reader(indexDirectory));
  for (size_t i = 0; i < 100; i++) {
    ASSERT_NO_THROW(ffi_query_sentence_bitmap(indexDirectory, column_names[0], generateRandomNormalString(i)));
    ASSERT_NO_THROW(ffi_query_sentence_bitmap(indexDirectory, column_names[1], generateRandomNormalString(i)));
    ASSERT_NO_THROW(ffi_query_sentence_bitmap(indexDirectory, column_names[2], generateRandomNormalString(i)));
  }
}

TEST_F(BoundaryFFiSearchTest, ffiQuerySentenceBitmapNullptrParameter) {
  ASSERT_ANY_THROW(ffi_query_sentence_bitmap(nullptr, column_names[0], generateRandomNormalString(10)));
  ASSERT_ANY_THROW(ffi_query_sentence_bitmap(indexDirectory, column_names[0], nullptr));
  ASSERT_ANY_THROW(ffi_query_sentence_bitmap(nullptr, column_names[0], nullptr));
}
