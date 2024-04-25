#include <BenchmarkConfig.h>

const std::string QueryTerms::TANTIVY_INDEX_FILES_PATH = "/tmp/tantivy_search/benchmark/index_path";
std::vector<std::string> QueryTerms::QUERY_TERMS;
std::vector<std::string> QueryTerms::QUERY_PATTERNS;
std::vector<std::vector<std::string>> QueryTerms::QUERY_5TERMS_SETS;
std::vector<std::vector<std::string>> QueryTerms::QUERY_10TERMS_SETS;
std::vector<std::string> QueryTerms::QUERY_SENTENCE_WITHIN_5_TERMS;
std::vector<std::string> QueryTerms::QUERY_SENTENCE_WITHIN_10_TERMS;
std::vector<std::vector<uint8_t>> QueryTerms::ALIVED_U8_BITMAP;
std::vector<Doc> QueryTerms::DOCUMENTS;

void QueryTerms::initializeQueryTerms() {
    QUERY_TERMS = WikiDatasetLoader::getInstance().loadQueryTerms();
    QUERY_PATTERNS = generatePatterns(QUERY_TERMS);
    QUERY_5TERMS_SETS = splitVector(QUERY_TERMS, 5);
    QUERY_10TERMS_SETS = splitVector(QUERY_TERMS, 10);
    QUERY_SENTENCE_WITHIN_5_TERMS = joinStrings(QUERY_TERMS, 5);
    QUERY_SENTENCE_WITHIN_10_TERMS = joinStrings(QUERY_TERMS, 10);
    ALIVED_U8_BITMAP = generateAlived1KBitmaps();
    DOCUMENTS = WikiDatasetLoader::getInstance().loadDocs();
}

std::vector<std::vector<std::string>> QueryTerms::splitVector(const std::vector<std::string>& input, int count) {
    std::vector<std::vector<std::string>> result;
    
    int numSubArrays = input.size() / count;
    if (input.size() % count != 0) {
        numSubArrays++;
    }
    
    for (int i = 0; i < numSubArrays; ++i) {
        std::vector<std::string> subArray;
        for (int j = 0; j < count && (i * count + j) < input.size(); ++j) {
            subArray.push_back(input[i * count + j]);
        }
        result.push_back(subArray);
    }
    
    return result;
}

std::vector<std::string> QueryTerms::joinStrings(const std::vector<std::string>& input, int count) {
    std::vector<std::string> result;
    
    for (size_t i = 0; i < input.size(); i += count) {
        std::string joinedString;
        for (int j = 0; j < count && (i + j) < input.size(); ++j) {
            joinedString += input[i + j];
            if (j < count - 1 && (i + j + 1) < input.size()) {
                joinedString += " ";
            }
        }
        result.push_back(joinedString);
    }
    
    return result;
}

std::vector<std::string> QueryTerms::generatePatterns(const std::vector<std::string>& input) {
    std::vector<std::string> patterns;
    for (size_t i = 0; i < input.size(); i++)
    {
        patterns.emplace_back(addRandomChars(input[i]));
    }
    return patterns;
}


int QueryTerms::getRandomInt(int maxCount) {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, maxCount);
    return dis(gen);
}

std::string QueryTerms::addRandomChars(const std::string& input) {
    std::string result = input;
    int numChars = getRandomInt(3);
    for (int i = 0; i < numChars; ++i) {
        int pos = getRandomInt(result.length());
        char randomChar = (getRandomInt(2) == 0) ? '%' : '_'; // random choose % or _
        result.insert(pos, 1, randomChar);
    }
    return result;
}

std::vector<std::vector<uint8_t>> QueryTerms::generateAlived1KBitmaps() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32_t> dis(0, 5000000);

    std::vector<std::vector<uint8_t>> u8bitmaps(1000, std::vector<uint8_t>(625000, 0)); // Each uint8 vector contains 625000 elements.

    for (int i = 0; i < 1000; ++i) {
        for (int j = 0; j < 10000; ++j) {
            uint32_t row_id = dis(gen);
            int index = row_id / 8;
            int bit_index = row_id % 8;
            u8bitmaps[i][index] |= (1 << bit_index);
        }
    }

    return u8bitmaps;
}