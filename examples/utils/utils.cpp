// utils.cpp
#include <iostream>
#include "utils.h"
#include <iostream>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <easylogging++.h>
using namespace std;

INITIALIZE_EASYLOGGINGPP

namespace Utils {

    int log_level = 0;

    std::string getCurrentDateTime() {
        auto now = std::chrono::system_clock::now(); // get current time.
        auto now_c = std::chrono::system_clock::to_time_t(now); // convert to `time_t`
        std::ostringstream oss;
        oss << std::put_time(std::localtime(&now_c), "%Y-%m-%d %H:%M:%S");
        return oss.str();
    }

    void tantivy_easylogging_callback(int level, const char* thread_id, const char* thread_name, const char* message)
    {
        string threadId(thread_id);
        string threadName(thread_name);
        string msg(message);


        switch(level) {
            case -2: // -2 -> fatal
                if (log_level >= -2 )
                    LOG(FATAL) << "[" << threadId << ":" << threadName << "]" << msg;
                break;
            case -1: // -1 -> error
                if (log_level >= -1 )
                    LOG(ERROR) << "[" << threadId << ":" << threadName << "]" << msg;
                break;
            case 0: // 0 -> warning
                if (log_level >= 0 )
                    LOG(WARNING) << "[" << threadId << ":" << threadName << "]" << msg;
                break;
            case 1: // 1 -> info
                if (log_level >= 1 )
                    LOG(INFO) << "[" << threadId << ":" << threadName << "]" << msg;
                break;
            case 2: // 2 -> debug
                if (log_level >= 2 )
                    LOG(DEBUG) << "[" << threadId << ":" << threadName << "]" << msg;
                break;
            case 3: // 3 -> tracing
                if (log_level >= 3 )
                    LOG(TRACE) << "[" << threadId << ":" << threadName << "]" << msg;
                break;
            default:
                LOG(DEBUG) << "[" << threadId << ":" << threadName << "]" << msg;
        }
    };

    std::vector<uint64_t> generate_array(std::size_t step, std::size_t lrange, std::size_t rrange) {
        std::vector<uint64_t> array;
        std::size_t size = (rrange - lrange) / step + 1; 
        array.reserve(size);

        for (uint64_t i = lrange; i <= rrange; i += step) {
            array.push_back(i);
        }

        return array;
    }

    void initialize_easy_logger(el::Level log_level) {
        el::Configurations defaultConf;
        defaultConf.setToDefault();
        // formatter
        defaultConf.set(log_level, el::ConfigurationType::Format, "%datetime %level %msg");
        // write to file
        defaultConf.set(log_level, el::ConfigurationType::ToFile, "true");
        // std out
        defaultConf.set(log_level, el::ConfigurationType::ToStandardOutput, "true");
        // apply config to logger
        el::Loggers::reconfigureLogger("default", defaultConf);

        LOG(INFO) << "initialize easylogger finished.";
    }
} // namespace Utils
