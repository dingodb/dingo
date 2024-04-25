#pragma once

#include <nlohmann/json.hpp>
#include <iostream>
#include <fstream>
#include <vector>
#include <mutex>
#include <string>
#include <tantivy_search.h>
#include <random>
#include <ctime>
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
    std::vector<Doc> loadDocs();

    void setIndexDirectory(const std::string& index_directory);
    std::string getIndexDirectory();

    void setDatasetFilePath(const std::string& dataset_file_path);
    std::string getDatasetFilePath();

    std::vector<uint64_t> getRowIdRanges(size_t index_granularity);
    std::vector<size_t> generateRandomArray(int length, int min, int max);

private:
    WikiDatasetLoader(){};
    size_t wiki_total_docs = 5600000; // wiki stored ≈ 560w rows doc

    std::string query_terms_file_path;
    std::string dataset_file_path;

    std::string index_directory; // directory stored index files
    std::vector<std::string> query_terms; // stored ≈ 10w terms for search
    std::vector<size_t> row_id_range;
    std::vector<Doc> docs;

    // mutexes for thread-safety
    std::mutex query_terms_mutex;
    std::mutex docs_mutex;
};
