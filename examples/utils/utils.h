#include <iostream>
#include <vector>
#include <set>
#include <random>
#include <stdexcept>
#include <algorithm>
#include <easylogging++.h>
// utils.h
#ifndef UTILS_H
#define UTILS_H

namespace Utils {
    extern int log_level;

    extern "C" void tantivy_easylogging_callback(int level, const char* thread_id, const char* thread_name, const char* message);

    std::vector<uint64_t> generate_array(std::size_t step, std::size_t lrange, std::size_t rrange);

    void initialize_easy_logger(el::Level log_level_);

    template <typename T>
    size_t intersection_size(const std::vector<T>& v1, const std::vector<T>& v2){
        std::set<T> set1(v1.begin(), v1.end());
        std::set<T> set2(v2.begin(), v2.end());
        std::vector<T> intersection_result;

        std::set_intersection(set1.begin(), set1.end(), set2.begin(), set2.end(), std::back_inserter(intersection_result));
        return intersection_result.size();
    }

    template <typename IN, typename OUT>
    std::vector<OUT> randomExtractK(const std::vector<IN>& input, size_t k){
        std::vector<OUT> result;
        if(k > input.size())
        {
            throw std::invalid_argument("`k` can't be smaller than the size of input vector");
        }
        // init random
        std::random_device rd;
        std::mt19937 generator(rd());

        // temp store duplicate of input
        std::vector<IN> temp = input;
        // shuffle temp vector
        std::shuffle(temp.begin(), temp.end(), generator);

        // choose first top K.
        for (size_t i = 0; i < k; i++)
        {
            result.push_back(static_cast<OUT>(temp[i]));
        }
        return result;
    }
} // namespace Utils

#endif // UTILS_H
