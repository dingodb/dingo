#include <iostream>
#include <vector>
#include <easylogging++.h>
// utils.h
#ifndef UTILS_H
#define UTILS_H

namespace Utils {

    static size_t log_level=1;

    extern "C" void tantivy_log_callback(int level, const char* thread_id, const char* thread_name, const char* message);

    void mylog(int level,  std::string message);

    extern "C" void tantivy_easylogging_callback(int level, const char* thread_id, const char* thread_name, const char* message);

    std::vector<uint64_t> generate_array(std::size_t step, std::size_t lrange, std::size_t rrange);

    void initialize_easy_logger(el::Level log_level);
} // namespace Utils

#endif // UTILS_H
