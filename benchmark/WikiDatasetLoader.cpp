#include <WikiDatasetLoader.h>

// from json to Doc.
void from_json(const json &j, Doc &doc)
{
    j.at("id").get_to(doc.id);
    j.at("title").get_to(doc.title);
    j.at("body").get_to(doc.body);
}

std::vector<uint64_t> generate_rowid_range(std::size_t step, std::size_t lrange, std::size_t rrange) {
    std::vector<uint64_t> array;
    std::size_t size = (rrange - lrange) / step + 1; 
    array.reserve(size);

    for (uint64_t i = lrange; i <= rrange; i += step) {
        array.push_back(i);
    }

    return array;
}

size_t index_docs_from_json(const std::string &raw_docs_file_path, const std::string &index_files_directory)
{
    // recreate directory
    if (fs::exists(index_files_directory)) {
        fs::remove_all(index_files_directory);
    }
    if (!fs::create_directories(index_files_directory)) {
        std::cerr << "Failed to create directory: " << index_files_directory << std::endl;
    }
    // load docs
    std::vector<Doc> docs = WikiDatasetLoader::getInstance().loadDocs(raw_docs_file_path);
    ffi_create_index(index_files_directory, {"text"});

    // index all docs
    size_t row_id = 0;
    for (const auto &doc : docs)
    {
        ffi_index_multi_column_docs(index_files_directory, row_id,  {"text"}, {doc.body.c_str()});
        row_id += 1;
    }
    ffi_index_writer_commit(index_files_directory);
    ffi_free_index_writer(index_files_directory);
    return row_id;
}

// Dataset loader for wiki dataset
WikiDatasetLoader& WikiDatasetLoader::getInstance() {
    static WikiDatasetLoader instance;
    return instance;
}

std::vector<std::string> WikiDatasetLoader::loadQueryTerms(const std::string& file_path) {
    // thread-safe
    std::lock_guard<std::mutex> lock(query_terms_mutex);
    // different file path, reload
    if (file_path!=query_terms_file_path){
        std::ifstream file(file_path);
        json j;
        file >> j;
        query_terms = j["terms"];
        file.close();
        query_terms_file_path = file_path;
    }
    return query_terms;
}

std::vector<std::string> WikiDatasetLoader::loadQueryTerms(){
    return loadQueryTerms(query_terms_file_path);
}

std::vector<Doc> WikiDatasetLoader::loadDocs(const std::string& file_path) {
    // thread-safe
    std::lock_guard<std::mutex> lock(docs_mutex);
    // different file path, reload
    if (file_path!=dataset_file_path) {
        std::ifstream file(file_path);
        // parase JSON
        json j;
        file >> j;
        // from json file to Doc vector
        docs = j.get<std::vector<Doc>>();
        file.close();
        dataset_file_path = file_path;
    }
    return docs;
}

std::vector<Doc> WikiDatasetLoader::loadDocs() {
    // thread-safe
    std::lock_guard<std::mutex> lock(docs_mutex);
    // different file path, reload
    std::ifstream file(this->dataset_file_path);
    // parase JSON
    json j;
    file >> j;
    // from json file to Doc vector
    docs = j.get<std::vector<Doc>>();
    file.close();
    return docs;
}

void WikiDatasetLoader::setIndexDirectory(const std::string& index_directory) {
        this->index_directory = index_directory;
    }
std::string WikiDatasetLoader::getIndexDirectory(){
    return this->index_directory;
}

void WikiDatasetLoader::setDatasetFilePath(const std::string& dataset_file_path){
    this->dataset_file_path = dataset_file_path;
}

std::string WikiDatasetLoader::getDatasetFilePath(){
    return this->dataset_file_path;
}

std::vector<uint64_t> WikiDatasetLoader::getRowIdRanges(size_t index_granularity){
    return generate_rowid_range(index_granularity, 0, wiki_total_docs);
}

std::vector<size_t> WikiDatasetLoader::generateRandomArray(int length, int min, int max) {
    if (min > max) std::swap(min, max);
    std::mt19937 rng(static_cast<unsigned int>(time(nullptr)));
    std::uniform_int_distribution<int> dist(min, max);
    std::vector<size_t> randomArray;
    randomArray.reserve(length);

    for (int i = 0; i < length; ++i) {
        randomArray.push_back(dist(rng));
    }

    return randomArray;
}
