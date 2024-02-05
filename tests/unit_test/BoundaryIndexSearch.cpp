#include <gtest/gtest.h>
#include <iostream>
#include <vector>
#include <tantivy_search.h>
#include <utils.h>
#include <filesystem>

namespace fs = std::filesystem;
using namespace Utils;
using namespace std;


class BoundaryTantivySearchReaderLoadTest : public ::testing::Test {
protected:
    const string logPath = "./log";
    const string indexDirectory = "./temp";
    const string indexDirectoryNotExists = "./temp2";
    void SetUp(){
        ASSERT_TRUE(tantivy_search_log4rs_initialize(logPath.c_str(), "info", true, false, true));
        ASSERT_TRUE(tantivy_create_index(indexDirectory, false));
    }
    void TearDown(){
        ASSERT_TRUE(tantivy_writer_free(indexDirectory));
        fs::remove_all(indexDirectory);
        fs::remove_all(indexDirectoryNotExists);
    }
};

TEST_F(BoundaryTantivySearchReaderLoadTest, readerLoadAndFreeWithExistsIndex) {
    for (size_t i = 0; i < 100; i++)
    {
        ASSERT_TRUE(tantivy_load_index(indexDirectory));
        ASSERT_TRUE(tantivy_load_index(indexDirectory));

        ASSERT_TRUE(tantivy_reader_free(indexDirectory));
        ASSERT_FALSE(tantivy_reader_free(indexDirectory));
    }
}

TEST_F(BoundaryTantivySearchReaderLoadTest, readerLoadAndFreeWithoutIndex) {
    ASSERT_ANY_THROW(tantivy_load_index(indexDirectoryNotExists));
    ASSERT_FALSE(tantivy_reader_free(indexDirectoryNotExists));
}


class BoundaryTantivySearchWithOutDocStoreTest : public ::testing::Test, public BoundaryUnitTestUtils {
protected:
    const string logPath = "./log";
    const string indexDirectory = "./temp";
    const string indexDirectoryNotExists = "./temp2";
    const string indexEmptyDirectory = "./temp3";
    void SetUp(){
        ASSERT_TRUE(tantivy_search_log4rs_initialize(logPath.c_str(), "info", true, false, true));
        ASSERT_TRUE(tantivy_create_index(indexEmptyDirectory, false));
        ASSERT_TRUE(tantivy_load_index(indexEmptyDirectory));
        ASSERT_TRUE(tantivy_create_index(indexDirectory, false));
        // Index 2w docs, each doc length is 1k.
        // u32 range: 0 ~ 4294967295
        for (uint64_t i = 4294960000; i < 4294980000; i++)
        {
            ASSERT_TRUE(tantivy_index_doc(indexDirectory, i, generateRandomString(1000)));
        }
        ASSERT_TRUE(tantivy_writer_commit(indexDirectory));
        ASSERT_TRUE(tantivy_load_index(indexDirectory));
    }
    void TearDown(){
        ASSERT_TRUE(tantivy_writer_free(indexDirectory));
        ASSERT_TRUE(tantivy_writer_free(indexEmptyDirectory));
        fs::remove_all(indexDirectory);
        fs::remove_all(indexDirectoryNotExists);
        fs::remove_all(indexEmptyDirectory);
    }  
};

TEST_F(BoundaryTantivySearchWithOutDocStoreTest, tantivySearchAndCountInRowIdRangeU32NotOverflow) {
    for (uint32_t i = 4294960000; i < (4294960000 + 1000); i++)
    {
        ASSERT_NO_THROW(tantivy_search_in_rowid_range(indexDirectory, generateRandomString(10), i-1000, i+1000, true));
        ASSERT_NO_THROW(tantivy_count_in_rowid_range(indexDirectory, generateRandomString(10), i-1000, i+1000, true));
        ASSERT_NO_THROW(tantivy_search_in_rowid_range(indexDirectory, generateRandomString(200), i-1000, i+5000, false));
        ASSERT_NO_THROW(tantivy_count_in_rowid_range(indexDirectory, generateRandomString(200), i-1000, i+5000, false));
    }
}

TEST_F(BoundaryTantivySearchWithOutDocStoreTest, tantivySearchInRowIdRangeInvalidRange) {
    ASSERT_ANY_THROW(tantivy_search_in_rowid_range(indexDirectory, generateRandomString(10), 100, 10, true));
}

TEST_F(BoundaryTantivySearchWithOutDocStoreTest, tantivySearchInRowIdRangeU64Overflow) {
    for (uint64_t i = 4294968000; i < (4294968000 + 1000); i++)
    {
        ASSERT_ANY_THROW(tantivy_search_in_rowid_range(indexDirectory, generateRandomString(10), i, i+1000, true));
        ASSERT_ANY_THROW(tantivy_count_in_rowid_range(indexDirectory, generateRandomString(10), i, i+1000, true));
        ASSERT_ANY_THROW(tantivy_search_in_rowid_range(indexDirectory, generateRandomString(200), i, i+5000, false));
        ASSERT_ANY_THROW(tantivy_count_in_rowid_range(indexDirectory, generateRandomString(200), i, i+5000, false));
        ASSERT_ANY_THROW(tantivy_search_in_rowid_range(indexDirectory, generateRandomString(5000), i, i+8000, true));
        ASSERT_ANY_THROW(tantivy_count_in_rowid_range(indexDirectory, generateRandomString(5000), i, i+8000, true));
        ASSERT_ANY_THROW(tantivy_search_in_rowid_range(indexDirectory, generateRandomString(10000), i, i+10000, false));
        ASSERT_ANY_THROW(tantivy_count_in_rowid_range(indexDirectory, generateRandomString(10000), i, i+10000, false));
    }
}

TEST_F(BoundaryTantivySearchWithOutDocStoreTest, tantivySearchInRowIdRangeWithoutReader) {
    ASSERT_TRUE(tantivy_reader_free(indexDirectory));
    ASSERT_ANY_THROW(tantivy_search_in_rowid_range(indexDirectory, generateRandomString(100), 4294960000, 4294960000+1000, true));
    ASSERT_ANY_THROW(tantivy_count_in_rowid_range(indexDirectory, generateRandomString(100), 4294960000, 4294960000+1000, true));
    ASSERT_ANY_THROW(tantivy_search_in_rowid_range(indexDirectory, generateRandomString(100), 4294960000, 4294960000+1000, false));
    ASSERT_ANY_THROW(tantivy_count_in_rowid_range(indexDirectory, generateRandomString(100), 4294960000, 4294960000+1000, false));
    ASSERT_TRUE(tantivy_load_index(indexDirectory));
    ASSERT_NO_THROW(tantivy_search_in_rowid_range(indexDirectory, generateRandomString(100), 4294960000, 4294960000+1000, true));
    ASSERT_NO_THROW(tantivy_count_in_rowid_range(indexDirectory, generateRandomString(100), 4294960000, 4294960000+1000, true));
    ASSERT_NO_THROW(tantivy_search_in_rowid_range(indexDirectory, generateRandomString(100), 4294960000, 4294960000+1000, false));
    ASSERT_NO_THROW(tantivy_count_in_rowid_range(indexDirectory, generateRandomString(100), 4294960000, 4294960000+1000, false));
    ASSERT_TRUE(tantivy_reader_free(indexDirectory));
}

TEST_F(BoundaryTantivySearchWithOutDocStoreTest, tantivySearchInRowIdRangeWithoutIndex) {
    ASSERT_ANY_THROW(tantivy_search_in_rowid_range(indexDirectoryNotExists, generateRandomString(100), 4294960000, 4294960000+1000, true));
    ASSERT_ANY_THROW(tantivy_count_in_rowid_range(indexDirectoryNotExists, generateRandomString(100), 4294960000, 4294960000+1000, true));
    ASSERT_ANY_THROW(tantivy_search_in_rowid_range(indexDirectoryNotExists, generateRandomString(100), 4294960000, 4294960000+1000, false));
    ASSERT_ANY_THROW(tantivy_count_in_rowid_range(indexDirectoryNotExists, generateRandomString(100), 4294960000, 4294960000+1000, false));
}

TEST_F(BoundaryTantivySearchWithOutDocStoreTest, tantivySearchInRowIdRangeWithEmptyIndex) {
    ASSERT_FALSE(tantivy_search_in_rowid_range(indexEmptyDirectory, generateRandomString(100), 4294960000, 4294960000+1000, true));
    ASSERT_TRUE(tantivy_count_in_rowid_range(indexEmptyDirectory, generateRandomString(100), 4294960000, 4294960000+1000, true)==0);
    ASSERT_FALSE(tantivy_search_in_rowid_range(indexEmptyDirectory, generateRandomString(100), 4294960000, 4294960000+1000, false));
    ASSERT_TRUE(tantivy_count_in_rowid_range(indexEmptyDirectory, generateRandomString(100), 4294960000, 4294960000+1000, false)==0);
}

TEST_F(BoundaryTantivySearchWithOutDocStoreTest, tantivyBM25Search) {
    for (size_t i = 0; i < 100; i++)
    {
        ASSERT_NO_THROW(tantivy_bm25_search(indexDirectory, generateRandomNormalString(i), 10, false));
        ASSERT_NO_THROW(tantivy_bm25_search(indexDirectory, generateRandomNormalString(i), 1000000, false));
        ASSERT_ANY_THROW(tantivy_bm25_search(indexDirectory, generateRandomNormalString(i), 10, true));
        ASSERT_ANY_THROW(tantivy_bm25_search(indexDirectory, generateRandomNormalString(i), 100000, true));
    }
}

TEST_F(BoundaryTantivySearchWithOutDocStoreTest, tantivyBM25SearchWithoutReader) {
    ASSERT_TRUE(tantivy_reader_free(indexDirectory));
    ASSERT_ANY_THROW(tantivy_bm25_search(indexDirectory, generateRandomNormalString(100), 10, false));
    ASSERT_ANY_THROW(tantivy_bm25_search(indexDirectory, generateRandomNormalString(100), 1000000, false));
    ASSERT_ANY_THROW(tantivy_bm25_search(indexDirectory, generateRandomNormalString(100), 10, true));
    ASSERT_ANY_THROW(tantivy_bm25_search(indexDirectory, generateRandomNormalString(100), 100000, true));
    ASSERT_TRUE(tantivy_load_index(indexDirectory));
    ASSERT_NO_THROW(tantivy_bm25_search(indexDirectory, generateRandomNormalString(100), 10, false));
    ASSERT_NO_THROW(tantivy_bm25_search(indexDirectory, generateRandomNormalString(100), 1000000, false));
    ASSERT_ANY_THROW(tantivy_bm25_search(indexDirectory, generateRandomNormalString(100), 10, true));
    ASSERT_ANY_THROW(tantivy_bm25_search(indexDirectory, generateRandomNormalString(100), 100000, true));
    ASSERT_TRUE(tantivy_reader_free(indexDirectory));
}

TEST_F(BoundaryTantivySearchWithOutDocStoreTest, tantivyBM25SearchWithoutIndex) {
    ASSERT_ANY_THROW(tantivy_bm25_search(indexDirectoryNotExists, generateRandomNormalString(100), 10, false));
    ASSERT_ANY_THROW(tantivy_bm25_search(indexDirectoryNotExists, generateRandomNormalString(100), 1000000, false));
    ASSERT_ANY_THROW(tantivy_bm25_search(indexDirectoryNotExists, generateRandomNormalString(100), 10, true));
    ASSERT_ANY_THROW(tantivy_bm25_search(indexDirectoryNotExists, generateRandomNormalString(100), 100000, true));
}

TEST_F(BoundaryTantivySearchWithOutDocStoreTest, tantivyBM25SearchWithEmptyIndex) {
    ASSERT_NO_THROW({
        auto resultsNoRegex = tantivy_bm25_search(indexEmptyDirectory, generateRandomNormalString(100), 10, false);
        ASSERT_TRUE(resultsNoRegex.size()==0);
    });
    ASSERT_NO_THROW(tantivy_bm25_search(indexEmptyDirectory, generateRandomNormalString(100), 100000, false));
    ASSERT_ANY_THROW(tantivy_bm25_search(indexEmptyDirectory, generateRandomNormalString(100), 100000, true));
}

TEST_F(BoundaryTantivySearchWithOutDocStoreTest, tantivyBM25SearchWithFilter) {
    for (size_t i = 0; i < 100; i++)
    {
        ASSERT_NO_THROW(tantivy_bm25_search_with_filter(indexDirectory, generateRandomNormalString(i), generateRandomUInt8Vector(i), 10, false));
        ASSERT_NO_THROW(tantivy_bm25_search_with_filter(indexDirectory, generateRandomNormalString(i), generateRandomUInt8Vector(i), 1000000, false));
        ASSERT_ANY_THROW(tantivy_bm25_search_with_filter(indexDirectory, generateRandomNormalString(i), generateRandomUInt8Vector(i), 10, true));
        ASSERT_ANY_THROW(tantivy_bm25_search_with_filter(indexDirectory, generateRandomNormalString(i), generateRandomUInt8Vector(i), 100000, true));
    }
}

TEST_F(BoundaryTantivySearchWithOutDocStoreTest, tantivySearchBitmapResults) {
    for (size_t i = 0; i < 100; i++)
    {
        ASSERT_NO_THROW(tantivy_search_bitmap_results(indexDirectory, generateRandomNormalString(i), true));
        ASSERT_NO_THROW(tantivy_search_bitmap_results(indexDirectory, generateRandomNormalString(i), false));
    }
}

TEST_F(BoundaryTantivySearchWithOutDocStoreTest, tantivySearchBitmapResultsWithEmptyIndex) {
    for (size_t i = 0; i < 100; i++)
    {
        ASSERT_NO_THROW({
            auto results = tantivy_search_bitmap_results(indexEmptyDirectory, generateRandomNormalString(i), true);
            ASSERT_TRUE(results.size()==0);
        });
        ASSERT_NO_THROW(tantivy_search_bitmap_results(indexEmptyDirectory, generateRandomNormalString(i), false));
    }
}

TEST_F(BoundaryTantivySearchWithOutDocStoreTest, tantivySearchBitmapResultsWithoutIndex) {
    for (size_t i = 0; i < 100; i++)
    {
        ASSERT_ANY_THROW(tantivy_search_bitmap_results(indexDirectoryNotExists, generateRandomNormalString(i), true));
        ASSERT_ANY_THROW(tantivy_search_bitmap_results(indexDirectoryNotExists, generateRandomNormalString(i), false));
    }
}

TEST_F(BoundaryTantivySearchWithOutDocStoreTest, tantivySearchBitmapResultsWithoutReader) {
    ASSERT_TRUE(tantivy_reader_free(indexDirectory));
    for (size_t i = 0; i < 100; i++)
    {
        ASSERT_ANY_THROW(tantivy_search_bitmap_results(indexDirectory, generateRandomNormalString(i), true));
        ASSERT_ANY_THROW(tantivy_search_bitmap_results(indexDirectory, generateRandomNormalString(i), false));
    }
    ASSERT_TRUE(tantivy_load_index(indexDirectory));
    for (size_t i = 0; i < 100; i++)
    {
        ASSERT_NO_THROW(tantivy_search_bitmap_results(indexDirectory, generateRandomNormalString(i), true));
        ASSERT_NO_THROW(tantivy_search_bitmap_results(indexDirectory, generateRandomNormalString(i), false));
    }
}


class BoundaryTantivySearchWithDocStoreTest : public ::testing::Test, public BoundaryUnitTestUtils {
protected:
    const string logPath = "./log";
    const string indexDirectory = "./temp";
    void SetUp(){
        ASSERT_TRUE(tantivy_search_log4rs_initialize(logPath.c_str(), "info", true, false, true));
        ASSERT_TRUE(tantivy_create_index(indexDirectory, true));
        for (uint64_t i = 0; i < 20000; i++)
        {
            ASSERT_TRUE(tantivy_index_doc(indexDirectory, i, generateRandomString(1000)));
        }
        ASSERT_TRUE(tantivy_writer_commit(indexDirectory));
        ASSERT_TRUE(tantivy_load_index(indexDirectory));
    }
    void TearDown(){
        ASSERT_TRUE(tantivy_writer_free(indexDirectory));
        fs::remove_all(indexDirectory);
    }  
};

TEST_F(BoundaryTantivySearchWithDocStoreTest, tantivyBM25Search) {
    ASSERT_NO_THROW(tantivy_bm25_search(indexDirectory, generateRandomNormalString(100), 10, false));
    ASSERT_NO_THROW(tantivy_bm25_search(indexDirectory, generateRandomNormalString(100), 1000000, false));
    ASSERT_NO_THROW(tantivy_bm25_search(indexDirectory, generateRandomNormalString(100), 10, true));
    ASSERT_NO_THROW(tantivy_bm25_search(indexDirectory, generateRandomNormalString(100), 100000, true));
}