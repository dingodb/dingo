#ifndef QUERY_TERMS_H
#define QUERY_TERMS_H

#include <iostream>
#include <vector>
#include <string>
#include <WikiDatasetLoader.h>

class QueryTerms {
public:
    static const std::string TANTIVY_INDEX_FILES_PATH;

    static std::vector<std::string> QUERY_TERMS;
    static std::vector<std::string> QUERY_PATTERNS;
    static std::vector<std::vector<std::string>> QUERY_5TERMS_SETS;
    static std::vector<std::vector<std::string>> QUERY_10TERMS_SETS;
    static std::vector<std::string> QUERY_SENTENCE_WITHIN_5_TERMS;
    static std::vector<std::string> QUERY_SENTENCE_WITHIN_10_TERMS;
    static std::vector<std::vector<uint8_t>> ALIVED_U8_BITMAP;
    static std::vector<Doc> DOCUMENTS;

    static void initializeQueryTerms();

private:
    static std::vector<std::vector<std::string>> splitVector(const std::vector<std::string>& input, int count);
    static std::vector<std::string> joinStrings(const std::vector<std::string>& input, int count);
    static std::vector<std::string> generatePatterns(const std::vector<std::string>& input);
    // Generate random u8 bitmaps.
    static std::vector<std::vector<uint8_t>> generateAlived1KBitmaps();
    // Generate random int from 0 to maxCount.
    static int getRandomInt(int maxCount);
    // Random insert '%' or '_'
    static std::string addRandomChars(const std::string& input);
};


#endif