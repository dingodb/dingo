#include <gtest/gtest.h>
#include <iostream>
#include <vector>
#include <tantivy_search.h>
#include <utils.h>
#include <filesystem>

using namespace Utils;
using namespace std;
namespace fs = std::filesystem;


class BoundaryTantivyCreateIndexWithTokenizerTest : public ::testing::Test {
protected:
    const string indexDirectory = "./temp";
    const string logPath = "./log";
    void SetUp() override {
        ASSERT_TRUE(tantivy_search_log4rs_init(logPath.c_str(), "info", false, false));
    }
    void TearDown() override {
        ASSERT_FALSE(tantivy_reader_free(indexDirectory));
        fs::remove_all(indexDirectory);
    }
    vector<string> generateTokenizerWithParameters(bool valid) {
        if (valid) {
            vector<string> validTokenizers = {
                "default", "raw",
                "simple", "simple(true)", "simple(false)", "SIMPLE(TRUE)", "SIMPLE(FALSE)",
                "whitespace", "whitespace(true)", "whitespace(false)", "WHITESPACE(TRUE)", "WHITESPACE(FALSE)",
                "en_stem", "en_stem(true)", "en_stem(false)", "EN_STEM(TRUE)", "EN_STEM(FALSE)",
                "ngram", "ngram(true)", "ngram(false)", "ngram(false, 2, 3)",  "ngram(false, 2)", "NGRAM(TRUE)", "NGRAM(FALSE)",
                "chinese", "chinese(default)", "chinese(empty)", "chinese(default, all)", "chinese(empty, unicode)", "chinese(default, default)", "chinese(empty, search)", 
                "chinese(default, all, true)", "chinese(empty, unicode, false)", "chinese(default, default, true)", "chinese(empty, search, false)", 
                "CHINESE(DEFAULT, ALL, TRUE)", "CHINESE(EMPTY, UNICODE, FALSE)", "CHINESE(DEFAULT)", "CHINESE(EMPTY)",
            };
            return validTokenizers;
        } else {
            vector<string> invalidTokenizers = {
                "super", "   ", " ", "-", "[ . . . ]", "💣", "()", "()()", "(())", "0(true)",
                "raw2", "raw ()", "raw(,)", "raw()()", "raw(1) (2)", "raw(12, 34)", "raw(true)", "raw(false)",
                "default2", "default ()", "default(,)", "default()()", "default(1) (2)", "default(12, 34)", "default(true)", "default(false)",
                "simp#le", "simple(,)", "simple(1)", "simple(1, 2)", "simple(True, 1)", "simple (false, 2, 3)", "simple(hello)",
                "white#space", "whitespace(,)", "whitespace(1)", "whitespace(1, 2)", "whitespace(True, 1)", "whitespace (false, 2, 3)", "whitespace(hello)",
                "en#stem", "en_stem(,)", "en_stem(1)", "en_stem(1, 2)", "en_stem(True, 1)", "en_stem (false, 2, 3)", "en_stem(hello)",
                "ngr.am", "ngram(,)", "ngram(1)", "ngram(false,5, 1)", "ngram(True, 1.3 ,2)", "ngram (false, 2, 3, biu)", "ngram(hello)",
                "- chine.se", "chinese(,)", "chinese(1)", "chinese(false,unicode,() hi)", "chinese(empty, all ,2)", "chinese (false, 2, 3, biu)", "chinese(hello)",
            };
            return invalidTokenizers;
        }
        
    } 
};

TEST_F(BoundaryTantivyCreateIndexWithTokenizerTest, ValidTokenizerPrameter) {
    vector<string> validTokenizers = generateTokenizerWithParameters(true);
    for (size_t i = 0; i < validTokenizers.size(); i++)
    {
        try
        {
            ASSERT_TRUE(tantivy_create_index_with_tokenizer(indexDirectory, validTokenizers[i], false));
            ASSERT_TRUE(tantivy_writer_free(indexDirectory));
        }
        catch(const std::exception& e)
        {
            ASSERT_TRUE(false);
        }
        
    }
}

TEST_F(BoundaryTantivyCreateIndexWithTokenizerTest, InvalidTokenizerPrameter) {
    vector<string> invalidTokenizers = generateTokenizerWithParameters(false);
    for (size_t i = 0; i < invalidTokenizers.size(); i++)
    {
        try
        {
            ASSERT_FALSE(tantivy_create_index_with_tokenizer(indexDirectory, invalidTokenizers[i], false));
            ASSERT_FALSE(tantivy_writer_free(indexDirectory));
        }
        catch(const std::exception& e)
        {
            std::string exp = e.what();
            ASSERT_TRUE( exp.find("tokenizer") != std::string::npos);
        }
    }
}


class BoundaryTantivyIndexDocTest : public ::testing::Test, public BoundaryUnitTestUtils {
protected:
    const string indexDirectory = "./temp";
    const string logPath = "./log";

    void SetUp() {
        ASSERT_TRUE(tantivy_search_log4rs_init(logPath.c_str(), "info", false, false));
        ASSERT_TRUE(tantivy_create_index(indexDirectory, false));
    }

    void TearDown() {
        ASSERT_FALSE(tantivy_reader_free(indexDirectory));
        ASSERT_TRUE(tantivy_writer_free(indexDirectory));
        fs::remove_all(indexDirectory);
    }
};

TEST_F(BoundaryTantivyIndexDocTest, index1wDocsWithDocLength100) {
    for (size_t i = 0; i < 10000; i++)
    {
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, i, generateRandomString(100)));
    }
    ASSERT_TRUE(tantivy_writer_commit(indexDirectory));
}

TEST_F(BoundaryTantivyIndexDocTest, index1wDocsWithDocLength1k) {
    for (size_t i = 0; i < 10000; i++)
    {
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, i, generateRandomString(1000)));
    }
    ASSERT_TRUE(tantivy_writer_commit(indexDirectory));
}

TEST_F(BoundaryTantivyIndexDocTest, index1wDocsWithDocLength1kStoredDoc) {
    ASSERT_TRUE(tantivy_create_index(indexDirectory, true));
    for (size_t i = 0; i < 10000; i++)
    {
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, i, generateRandomString(1000)));
    }
    ASSERT_TRUE(tantivy_writer_commit(indexDirectory));
}


class BoundaryTantivyDeleteRowIdsTest : public ::testing::Test, public BoundaryUnitTestUtils {
protected:
    const string indexDirectory = "./temp";
    const string indexDirectoryNotExists = "./temp2";
    const string indexEmptyDirectory= "./temp3";
    const string logPath = "./log";
    const size_t totalDocNums = 10000;

    void SetUp() override {
        ASSERT_TRUE(tantivy_search_log4rs_init(logPath.c_str(), "trace", false, false));
        ASSERT_TRUE(tantivy_create_index(indexDirectory, false));
        IndexDocuments(indexDirectory, totalDocNums);
        ASSERT_TRUE(tantivy_create_index(indexEmptyDirectory, false));
    }

    void TearDown() override {
        ASSERT_TRUE(tantivy_writer_free(indexDirectory));
        fs::remove_all(indexDirectory);
        fs::remove_all(indexDirectoryNotExists);
        fs::remove_all(indexEmptyDirectory);
    }

    void IndexDocuments(const string& _indexDirectory, size_t totalDocNums) {
        for (size_t i = 0; i < totalDocNums; i++) {
            string doc = generateRandomString(i % (totalDocNums / 100));
            ASSERT_TRUE(tantivy_index_doc(_indexDirectory, i, doc));
        }
    }

    vector<uint32_t> GenerateRowIdsToDelete(size_t totalDocNums, bool isFistDelete) {
        if (isFistDelete){
            vector<uint32_t> rowIdsFirstDelete = {
                0, 1, 2, 5000, 5001, 5002, 9000, 9001, 9002, 
                static_cast<uint32_t>(totalDocNums)+1, static_cast<uint32_t>(totalDocNums)+2 // delete not exist row_ids
            };
            return rowIdsFirstDelete;
        } else {
            vector<uint32_t> rowIdsSecondDelete = {
                0, 1, 2, 5000, 5001, 5002, 9000, 9001, 9002,                                 // delete row_ids second time
                0, 3, 4, 5, 5003, 5004, 5005, 9003, 9004, 9005, 
                static_cast<uint32_t>(totalDocNums)+3, static_cast<uint32_t>(totalDocNums)+4 // delete not exist row_ids
            };
            return rowIdsSecondDelete;
        }
        
    }
};

TEST_F(BoundaryTantivyDeleteRowIdsTest, Delete1kRowIds) {
    auto rowIdsFirstDelete = GenerateRowIdsToDelete(totalDocNums, true);
    ASSERT_TRUE(tantivy_delete_row_ids(indexDirectory, rowIdsFirstDelete));
    auto rowIdsSecondDelete = GenerateRowIdsToDelete(totalDocNums, false);
    ASSERT_TRUE(tantivy_delete_row_ids(indexDirectory, rowIdsSecondDelete));
    ASSERT_FALSE(tantivy_reader_free(indexDirectory));
}

TEST_F(BoundaryTantivyDeleteRowIdsTest, Delete1kRowIdsWithReader) {
    ASSERT_TRUE(tantivy_load_index(indexDirectory));
    auto rowIdsFirstDelete = GenerateRowIdsToDelete(totalDocNums, true);
    ASSERT_TRUE(tantivy_delete_row_ids(indexDirectory, rowIdsFirstDelete));
    auto rowIdsSecondDelete = GenerateRowIdsToDelete(totalDocNums, false);
    ASSERT_TRUE(tantivy_delete_row_ids(indexDirectory, rowIdsSecondDelete));
    ASSERT_TRUE(tantivy_reader_free(indexDirectory));
}

TEST_F(BoundaryTantivyDeleteRowIdsTest, Delete1kRowIdsWithEmptyIndex) {
    auto rowIdsDelete = GenerateRowIdsToDelete(10000, true);
    ASSERT_TRUE(tantivy_delete_row_ids(indexEmptyDirectory, rowIdsDelete));
}

TEST_F(BoundaryTantivyDeleteRowIdsTest, Delete1kRowIdsWithoutIndex) {
    auto rowIdsDelete = GenerateRowIdsToDelete(10000, true);
    ASSERT_ANY_THROW(tantivy_delete_row_ids(indexDirectoryNotExists, rowIdsDelete));
}


class BoundaryTantivyCreateAndFreeIndexTest : public ::testing::Test, public BoundaryUnitTestUtils {
protected:
    const string logPath = "./log";
    void SetUp() {
        ASSERT_TRUE(tantivy_search_log4rs_init(logPath.c_str(), "info", false, true));
    }
    void TearDown() {
        //
    }
};

TEST_F(BoundaryTantivyCreateAndFreeIndexTest, writerCreateAndFree) {
    for (size_t i = 0; i < 100; i++) {
        ASSERT_TRUE(tantivy_create_index("./temp//", false));
        ASSERT_TRUE(tantivy_writer_commit("./temp"));
        ASSERT_TRUE(tantivy_create_index("./temp", true));
        ASSERT_TRUE(tantivy_writer_commit("./temp"));
        ASSERT_TRUE(tantivy_create_index("./temp////", false));
        ASSERT_TRUE(tantivy_writer_commit("./temp"));
        ASSERT_TRUE(tantivy_writer_free("./temp///"));
        ASSERT_FALSE(tantivy_writer_free("./temp///"));
        ASSERT_FALSE(tantivy_writer_free("./temp"));
        ASSERT_FALSE(tantivy_writer_free("./abcd"));
    }
}

TEST_F(BoundaryTantivyCreateAndFreeIndexTest, writerAndReaderCreateAndFree) {
    for (size_t i = 0; i < 100; i++) {
        // Build index.
        ASSERT_TRUE(tantivy_create_index("./temp//", false));
        for (size_t i = 0; i < 1000; i++)
        {
            ASSERT_TRUE(tantivy_index_doc("./temp///", i, generateRandomString(100)));
        }
        ASSERT_TRUE(tantivy_writer_commit("./temp"));
        // Load index.
        ASSERT_TRUE(tantivy_load_index("./temp/////"));
        // No need to manually free readers and writers anymore.
    }
}

class BoundaryTantivyCommitTest : public ::testing::Test, public BoundaryUnitTestUtils {
protected:
    const string logPath = "./log";
    const string indexDirectory = "./temp";
    const string indexDirectoryNotExists = "./temp2";
    void SetUp() {
        ASSERT_TRUE(tantivy_search_log4rs_init(logPath.c_str(), "info", false, true));
        ASSERT_TRUE(tantivy_create_index(indexDirectory, false));
    }
    void TearDown() {
        ASSERT_TRUE(tantivy_writer_free(indexDirectory));
        ASSERT_FALSE(tantivy_reader_free(indexDirectory));
        fs::remove_all(indexDirectory);
        fs::remove_all(indexDirectoryNotExists);
    }
};

TEST_F(BoundaryTantivyCommitTest, commitNotExist) {
    for (size_t i = 0; i < 1000; i++)
    {
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, i, generateRandomString(100)));
    }
    ASSERT_TRUE(tantivy_writer_commit(indexDirectory));
    ASSERT_ANY_THROW(tantivy_writer_commit(indexDirectoryNotExists));

}
