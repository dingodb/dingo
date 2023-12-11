// utils.cpp
#include <iostream>
#include "utils.h"
using namespace std;


namespace Utils {

    

    std::vector<uint64_t> generate_array(std::size_t step, std::size_t lrange, std::size_t rrange) {
        std::vector<uint64_t> array;
        std::size_t size = (rrange - lrange) / step + 1; 
        array.reserve(size);

        for (uint64_t i = lrange; i <= rrange; i += step) {
            array.push_back(i);
        }

        return array;
    }
} // namespace Utils

int main(){
    // test_default_create();
    // test_regx_create();
    // test_tokenizer_create();
    return 0;
}