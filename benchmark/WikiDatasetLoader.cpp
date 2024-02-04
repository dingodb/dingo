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
    tantivy_create_index(index_files_directory, false);

    // index all docs
    size_t row_id = 0;
    for (const auto &doc : docs)
    {
        tantivy_index_doc(index_files_directory, row_id, doc.body.c_str());
        row_id += 1;
    }
    tantivy_writer_commit(index_files_directory);
    tantivy_writer_free(index_files_directory);
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
    if(query_terms.empty()){
        std::cout << "Query terms need initialize." << std::endl;
    }
    return query_terms;
}

std::vector<Doc> WikiDatasetLoader::loadDocs(const std::string& file_path) {
    // thread-safe
    std::lock_guard<std::mutex> lock(docs_mutex);
    // different file path, reload
    if (file_path!=doc_file_path) {
        std::ifstream file(file_path);
        // parase JSON
        json j;
        file >> j;
        // from json file to Doc vector
        docs = j.get<std::vector<Doc>>();
        file.close();
    }
    return docs;
}

void WikiDatasetLoader::setIndexDirectory(const std::string& index_directory) {
        this->index_directory = index_directory;
    }
std::string WikiDatasetLoader::getIndexDirectory(){
    return this->index_directory;
}

std::vector<uint64_t> WikiDatasetLoader::getRowIdRanges(){
    if(row_id_range.empty()){
        std::lock_guard<std::mutex> lock(row_id_range_mutex);
        if(row_id_range.empty()){
            row_id_range = generate_rowid_range(row_id_step, 0, wiki_total_docs);
        }
    }
    return row_id_range;
}

size_t WikiDatasetLoader::getGranuleStep(){
    if(row_id_step==0){
        return 8192;
    }
    return row_id_step;
}

