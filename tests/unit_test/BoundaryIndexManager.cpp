#include <gtest/gtest.h>
#include <iostream>
#include <vector>
#include <tantivy_search.h>
#include <utils.h>
#include <filesystem>

using namespace Utils;
using namespace std;
namespace fs = std::filesystem;


TEST(BoundaryTantivyLoggerTest, validParameter) {
    ASSERT_TRUE(tantivy_search_log4rs_initialize("./log", "info", true, false, false));
}

TEST(BoundaryTantivyLoggerTest, nullptrParameter) {
    ASSERT_FALSE(tantivy_search_log4rs_initialize(nullptr, "info", false, false, false));
    ASSERT_FALSE(tantivy_search_log4rs_initialize("./log", nullptr, false, false, false));
    ASSERT_FALSE(tantivy_search_log4rs_initialize(nullptr, nullptr, false, false, false));
}


class BoundaryTantivyCreateIndexWithTokenizerTest : public ::testing::Test {
protected:
    const string indexDirectory = "./temp";
    const string logPath = "./log";
    const vector<string> column_names = {"col1", "col2", "col3"};
    void SetUp() override {
        ASSERT_TRUE(tantivy_search_log4rs_initialize(logPath.c_str(), "info", true, false, false));
    }
    void TearDown() override {
        ASSERT_FALSE(ffi_free_index_reader(indexDirectory));
        fs::remove_all(indexDirectory);
    }
    vector<string> generateTokenizerWithParameters(bool valid) {
        if (valid) {
            vector<string> validTokenizers = {
                // "{}",
                // default tokenizer
                "{\"col1\":{\"tokenizer\":{\"type\":\"default\"}}}",
                "{\"col2\":{\"tokenizer\":{\"type\":\"default\",\"store_doc\":true}}}",
                "{\"col3\":{\"tokenizer\":{\"type\":\"default\",\"store_doc\":false}}}",
                // raw tokenizer
                "{\"col1\":{\"tokenizer\":{\"type\":\"raw\",\"store_doc\":false}}}",
                "{\"col2\":{\"tokenizer\":{\"type\":\"raw\",\"store_doc\":true}}}",
                "{\"col3\":{\"tokenizer\":{\"type\":\"raw\",\"store_doc\":true}}, \"col2\":{\"tokenizer\":{\"type\":\"raw\",\"store_doc\":false}}}",
                // simple tokenizer
                "{\"col1\":{\"tokenizer\":{\"type\":\"simple\",\"stop_word_filters\":[\"english\"],\"store_doc\":true,\"length_limit\":50,\"case_sensitive\":false}}}",
                // stem tokenizer
                "{\"col2\":{\"tokenizer\":{\"type\":\"stem\",\"stop_word_filters\":[\"english\",\"french\"],\"stem_languages\":[\"english\",\"french\"],\"store_doc\":true,\"length_limit\":60,\"case_sensitive\":true}}}",
                // whitespace tokenizer
                "{\"col3\":{\"tokenizer\":{\"type\":\"whitespace\",\"stop_word_filters\":[],\"store_doc\":false,\"length_limit\":30,\"case_sensitive\":false}}}",
                // ngram tokenizer
                "{\"col1\":{\"tokenizer\":{\"type\":\"ngram\",\"min_gram\":1,\"max_gram\":4,\"prefix_only\":false,\"stop_word_filters\":[\"english\"],\"store_doc\":true,\"length_limit\":40,\"case_sensitive\":true}}}",
                // chinese tokenizer
                "{\"col2\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"default\",\"mode\":\"search\",\"hmm\":false,\"store_doc\":true}}}"
            };
            return validTokenizers;
        } else {
            vector<string> invalidTokenizers = {
                // unknown
                "{\"col1\":{\"tokenizer\":{\"type\":\"invalid_type\"}}}",
                // lack necessary
                "{\"col2\":{\"tokenizer\":{\"store_doc\":true}}}",
                // invalid stop_word_filters value
                "{\"col3\":{\"tokenizer\":{\"type\":\"simple\",\"stop_word_filters\":[\"invalid_language\"]}}}",
                // invalid stem_languages value
                "{\"col1\":{\"tokenizer\":{\"type\":\"stem\",\"stem_languages\":[\"unsupported_language\"]}}}",
                // invalid jieba value
                "{\"col2\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"invalid_value\"}}}",
                // invalid mode value
                "{\"col3\":{\"tokenizer\":{\"type\":\"chinese\",\"mode\":\"invalid_mode\"}}}",
                // min_gram > max_gram
                "{\"col1\":{\"tokenizer\":{\"type\":\"ngram\",\"min_gram\":4,\"max_gram\":2}}}",
                // just invalid
                "hadjopew099-1ej1"
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
            ASSERT_TRUE(ffi_create_index_with_parameter(indexDirectory, column_names, validTokenizers[i]));
            ASSERT_TRUE(ffi_free_index_writer(indexDirectory));
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
            ASSERT_FALSE(ffi_create_index_with_parameter(indexDirectory, column_names, invalidTokenizers[i]));
            ASSERT_FALSE(ffi_free_index_writer(indexDirectory));
        }
        catch(const std::exception& e)
        {
            std::string exp = e.what();
            ASSERT_TRUE( exp.find("tokenizer") != std::string::npos);
        }
    }
}

TEST_F(BoundaryTantivyCreateIndexWithTokenizerTest, nullptrParameter) {
    ASSERT_ANY_THROW(ffi_create_index_with_parameter(indexDirectory, column_names, nullptr));
    ASSERT_FALSE(ffi_free_index_writer(indexDirectory));
    ASSERT_ANY_THROW(ffi_create_index_with_parameter(nullptr, column_names, "{}"));
    ASSERT_ANY_THROW(ffi_create_index_with_parameter(nullptr, {}, nullptr));
}


class BoundaryTantivyIndexDocTest : public ::testing::Test, public BoundaryUnitTestUtils {
protected:
    const string indexDirectory = "./temp";
    const string logPath = "./log";
    const vector<string> column_names = {"col1", "col2", "col3"};


    void SetUp() {
        ASSERT_TRUE(tantivy_search_log4rs_initialize(logPath.c_str(), "info", true, false, false));
        ASSERT_TRUE(ffi_create_index(indexDirectory, column_names));
    }

    void TearDown() {
        ASSERT_FALSE(ffi_free_index_reader(indexDirectory));
        ASSERT_TRUE(ffi_free_index_writer(indexDirectory));
        fs::remove_all(indexDirectory);
    }
};

TEST_F(BoundaryTantivyIndexDocTest, index1wDocsWithDocLength100) {
    for (size_t i = 0; i < 10000; i++)
    {
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, i, column_names, {generateRandomString(100), generateRandomString(100), generateRandomString(100)}));
    }
    ASSERT_TRUE(ffi_index_writer_commit(indexDirectory));
}

TEST_F(BoundaryTantivyIndexDocTest, index1wDocsWithDocLength1k) {
    for (size_t i = 0; i < 10000; i++)
    {
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, i, column_names, {generateRandomString(100), generateRandomString(100), generateRandomString(100)}));
    }
    ASSERT_TRUE(ffi_index_writer_commit(indexDirectory));
}

TEST_F(BoundaryTantivyIndexDocTest, index1wDocsWithDocLength1kStoredDoc) {
    ASSERT_TRUE(ffi_create_index(indexDirectory, column_names));
    for (size_t i = 0; i < 10000; i++)
    {
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, i, column_names, {generateRandomString(100), generateRandomString(100), generateRandomString(100)}));
    }
    ASSERT_TRUE(ffi_index_writer_commit(indexDirectory));
}

TEST_F(BoundaryTantivyIndexDocTest, nullptrParameter) {
    for (size_t i = 0; i < 10000; i++)
    {
        ASSERT_ANY_THROW(ffi_index_multi_column_docs(indexDirectory, i, {nullptr, nullptr}, {nullptr}));
        ASSERT_ANY_THROW(ffi_index_multi_column_docs(nullptr, i, {}, {}));
        ASSERT_ANY_THROW(ffi_index_multi_column_docs(nullptr, i, {nullptr}, {nullptr}));
    }
}


class BoundaryTantivyDeleteRowIdsTest : public ::testing::Test, public BoundaryUnitTestUtils {
protected:
    const string indexDirectory = "./temp";
    const string indexDirectoryNotExists = "./temp2";
    const string indexEmptyDirectory= "./temp3";
    const string logPath = "./log";
    const size_t totalDocNums = 10000;
    const vector<string> column_names = {"col1", "col2", "col3"};

    void SetUp() override {
        ASSERT_TRUE(tantivy_search_log4rs_initialize(logPath.c_str(), "trace", true, false, false));
        ASSERT_TRUE(ffi_create_index(indexDirectory, column_names));
        IndexDocuments(indexDirectory, totalDocNums);
        ASSERT_TRUE(ffi_create_index(indexEmptyDirectory, column_names));
    }

    void TearDown() override {
        ASSERT_TRUE(ffi_free_index_writer(indexDirectory));
        fs::remove_all(indexDirectory);
        fs::remove_all(indexDirectoryNotExists);
        fs::remove_all(indexEmptyDirectory);
    }

    void IndexDocuments(const string& _indexDirectory, size_t totalDocNums) {
        for (size_t i = 0; i < totalDocNums; i++) {
            string doc = generateRandomString(i % (totalDocNums / 100));
            ASSERT_TRUE(ffi_index_multi_column_docs(_indexDirectory, i, column_names, {doc, doc, doc}));
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
    ASSERT_TRUE(ffi_delete_row_ids(indexDirectory, rowIdsFirstDelete));
    auto rowIdsSecondDelete = GenerateRowIdsToDelete(totalDocNums, false);
    ASSERT_TRUE(ffi_delete_row_ids(indexDirectory, rowIdsSecondDelete));
    ASSERT_FALSE(ffi_free_index_reader(indexDirectory));
}

TEST_F(BoundaryTantivyDeleteRowIdsTest, Delete1kRowIdsWithReader) {
    ASSERT_TRUE(ffi_load_index_reader(indexDirectory));
    auto rowIdsFirstDelete = GenerateRowIdsToDelete(totalDocNums, true);
    ASSERT_TRUE(ffi_delete_row_ids(indexDirectory, rowIdsFirstDelete));
    auto rowIdsSecondDelete = GenerateRowIdsToDelete(totalDocNums, false);
    ASSERT_TRUE(ffi_delete_row_ids(indexDirectory, rowIdsSecondDelete));
    ASSERT_TRUE(ffi_free_index_reader(indexDirectory));
}

TEST_F(BoundaryTantivyDeleteRowIdsTest, Delete1kRowIdsWithEmptyIndex) {
    auto rowIdsDelete = GenerateRowIdsToDelete(10000, true);
    ASSERT_TRUE(ffi_delete_row_ids(indexEmptyDirectory, rowIdsDelete));
}

TEST_F(BoundaryTantivyDeleteRowIdsTest, Delete1kRowIdsWithoutIndex) {
    auto rowIdsDelete = GenerateRowIdsToDelete(10000, true);
    ASSERT_FALSE(ffi_delete_row_ids(indexDirectoryNotExists, rowIdsDelete));
}


class BoundaryTantivyCreateAndFreeIndexTest : public ::testing::Test, public BoundaryUnitTestUtils {
protected:
    const string logPath = "./log";
    const vector<string> column_names = {"col1", "col2", "col3"};

    void SetUp() {
        ASSERT_TRUE(tantivy_search_log4rs_initialize(logPath.c_str(), "info", true, false, true));
    }
    void TearDown() {
        //
    }
};

TEST_F(BoundaryTantivyCreateAndFreeIndexTest, writerCreateAndFree) {
    for (size_t i = 0; i < 100; i++) {
        ASSERT_TRUE(ffi_create_index("./temp//", column_names));
        ASSERT_TRUE(ffi_index_writer_commit("./temp"));
        ASSERT_TRUE(ffi_create_index("./temp", column_names));
        ASSERT_TRUE(ffi_index_writer_commit("./temp"));
        ASSERT_TRUE(ffi_create_index("./temp///", column_names));
        ASSERT_TRUE(ffi_index_writer_commit("./temp"));
        ASSERT_TRUE(ffi_free_index_writer("./temp///"));
        ASSERT_FALSE(ffi_free_index_writer("./temp///"));
        ASSERT_FALSE(ffi_free_index_writer("./temp"));
        ASSERT_FALSE(ffi_free_index_writer("./abcd"));
    }
    fs::remove_all("./temp");
}

TEST_F(BoundaryTantivyCreateAndFreeIndexTest, writerAndReaderCreateAndFree) {
    for (size_t i = 0; i < 100; i++) {
        // Build index.
        ASSERT_TRUE(ffi_create_index("./temp//", column_names));
        for (size_t i = 0; i < 1000; i++)
        {
            ASSERT_TRUE(ffi_index_multi_column_docs("./temp///", i, column_names, {generateRandomString(100),generateRandomString(100),generateRandomString(100)}));
        }
        ASSERT_TRUE(ffi_index_writer_commit("./temp"));
        // Stop merge.
        ASSERT_TRUE(ffi_free_index_writer("./temp///"));
        // Load index.
        ASSERT_TRUE(ffi_load_index_reader("./temp///"));
        // Free reader
        ASSERT_TRUE(ffi_free_index_reader("./temp///"));
    }
    fs::remove_all("./temp");
}

TEST_F(BoundaryTantivyCreateAndFreeIndexTest, nullptrParameter) {
    ASSERT_ANY_THROW(ffi_create_index(nullptr, column_names));
    ASSERT_ANY_THROW(ffi_index_multi_column_docs("./temp///", 0, {nullptr}, {nullptr}));
    ASSERT_ANY_THROW(ffi_index_multi_column_docs(nullptr, 1, {nullptr}, {nullptr}));
    ASSERT_ANY_THROW(ffi_index_writer_commit(nullptr));
    ASSERT_ANY_THROW(ffi_load_index_reader(nullptr));
}


class BoundaryTantivyCommitTest : public ::testing::Test, public BoundaryUnitTestUtils {
protected:
    const string logPath = "./log";
    const string indexDirectory = "./temp";
    const string indexDirectoryNotExists = "./temp2";
    const vector<string> column_names = {"col1", "col2", "col3"};

    void SetUp() {
        ASSERT_TRUE(tantivy_search_log4rs_initialize(logPath.c_str(), "info", true, false, true));
        ASSERT_TRUE(ffi_create_index(indexDirectory, column_names));
    }
    void TearDown() {
        ASSERT_TRUE(ffi_free_index_writer(indexDirectory));
        ASSERT_FALSE(ffi_free_index_reader(indexDirectory));
        fs::remove_all(indexDirectory);
        fs::remove_all(indexDirectoryNotExists);
    }
};

TEST_F(BoundaryTantivyCommitTest, commitNotExist) {
    for (size_t i = 0; i < 1000; i++)
    {
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, i, column_names, {generateRandomString(100), generateRandomString(100), generateRandomString(100)}));
    }
    ASSERT_TRUE(ffi_index_writer_commit(indexDirectory));
    ASSERT_FALSE(ffi_index_writer_commit(indexDirectoryNotExists));
}

TEST_F(BoundaryTantivyCommitTest, nullptrParameter) {
    ASSERT_ANY_THROW(ffi_index_multi_column_docs(nullptr, 0, {nullptr}, {nullptr}));
    ASSERT_ANY_THROW(ffi_index_multi_column_docs(indexDirectory, 1, {nullptr}, {nullptr}));
    ASSERT_ANY_THROW(ffi_index_writer_commit(nullptr));
}
