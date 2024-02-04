// utils.h
#ifndef UTILS_H
#define UTILS_H

#include <iostream>
#include <vector>
#include <easylogging++.h>

namespace Utils {
    extern int log_level;

    extern "C" void tantivy_easylogging_callback(int level, const char* thread_info, const char* message);

    std::vector<uint64_t> generate_array(std::size_t step, std::size_t lrange, std::size_t rrange);

    void initialize_easy_logger(el::Level log_level_);

    template <typename T>
    size_t intersection_size(const std::vector<T>& v1, const std::vector<T>& v2);

    template <typename IN, typename OUT>
    std::vector<OUT> randomExtractK(const std::vector<IN>& input, size_t k);

    class BoundaryUnitTestUtils {
    public:
        std::string generateRandomString(size_t length);
        std::string generateRandomNormalString(size_t length);
        std::vector<uint8_t> generateRandomUInt8Vector(size_t length);
    private:
        std::string generateRandomStringFromCharacters(size_t length, std::wstring characters);
    };
} // namespace Utils


#include "utils_template_impl.h"
#endif // UTILS_H
