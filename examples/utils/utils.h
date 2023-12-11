#include<iostream>
#include<vector>
// utils.h
#ifndef UTILS_H
#define UTILS_H

namespace Utils {

    extern "C" void tantivy_log_callback(int level, const char* thread_id, const char* thread_name, const char* message);

    std::vector<uint64_t> generate_array(std::size_t step, std::size_t lrange, std::size_t rrange);
} // namespace Utils

#endif // UTILS_H
