#pragma once

#include <nlohmann/json.hpp>
#include <iostream>
#include <fstream>
#include <vector>
#include <mutex>
#include <string>
#include <tantivy_search.h>

using json = nlohmann::json;
namespace fs = std::filesystem;

struct Doc {
    int id;
    std::string title;
    std::string body;
};

// from json to Doc.
void from_json(const json &j, Doc &doc);

// Forward declaration of WikiDatasetLoader
class WikiDatasetLoader;

std::vector<uint64_t> generate_rowid_range(std::size_t step, std::size_t lrange, std::size_t rrange);

size_t index_docs_from_json(const std::string &raw_docs_file_path, const std::string &index_files_directory);

class WikiDatasetLoader {
public:
    static WikiDatasetLoader& getInstance();

    WikiDatasetLoader(const WikiDatasetLoader&) = delete;
    void operator=(const WikiDatasetLoader&) = delete;

    std::vector<std::string> loadQueryTerms(const std::string& file_path);
    std::vector<std::string> loadQueryTerms();
    std::vector<Doc> loadDocs(const std::string& file_path);

    void setIndexDirectory(const std::string& index_directory);
    std::string getIndexDirectory();

    std::vector<uint64_t> getRowIdRanges();
    size_t getGranuleStep();

private:
    WikiDatasetLoader(){};
    size_t row_id_step = 8192;
    size_t wiki_total_docs = 5600000; // wiki stored ≈ 560w rows doc

    std::string query_terms_file_path;
    std::string doc_file_path;

    std::string index_directory; // directory stored index files
    std::vector<std::string> query_terms; // search some terms
    std::vector<size_t> row_id_range;
    std::vector<Doc> docs;

    // mutexes for thread-safety
    std::mutex query_terms_mutex;
    std::mutex docs_mutex;
    std::mutex row_id_range_mutex;
};
