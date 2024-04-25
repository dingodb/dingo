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
    std::string getCurrentDateTime() {
        // 获取当前时间点
        auto now = std::chrono::system_clock::now();
        // 转换为 time_t 类型
        auto now_c = std::chrono::system_clock::to_time_t(now);
        // 转换为 tm 结构
        struct tm *parts = std::localtime(&now_c);
        // 使用 stringstream 进行格式化
        std::ostringstream oss;
        oss << std::put_time(parts, "%Y-%m-%d %H:%M:%S");
        return oss.str();
    }


    void tantivy_log_callback(int level, const char* thread_id, const char* thread_name, const char* message)
    {
        string threadId(thread_id);
        string threadName(thread_name);
        string msg(message);


        switch(level) {
            case -2: // -2 -> fatal
                if (log_level >= -2 )
                    std::cout << getCurrentDateTime() <<" - FATAL [" + threadId + ":" + threadName + "] " + msg << endl;
                break;
            case -1: // -1 -> error
                if (log_level >= -1 )
                    std::cout << getCurrentDateTime() << " - ERROR [" + threadId + ":" + threadName + "] " + msg << endl;
                break;
            case 0: // 0 -> warning
                if (log_level >= 0 )
                    std::cout << getCurrentDateTime() << " - WARNING [" + threadId + ":" + threadName + "] " + msg << endl;
                break;
            case 1: // 1 -> info
                if (log_level >= 1 )
                    std::cout << getCurrentDateTime() << " - INFO [" + threadId + ":" + threadName + "] " + msg << endl;
                break;
            case 2: // 2 -> debug
                if (log_level >= 2 )
                    std::cout << getCurrentDateTime() << " - DEBUG [" + threadId + ":" + threadName + "] " + msg << endl;
                break;
            case 3: // 3 -> tracing
                if (log_level >= 3 )
                    std::cout << getCurrentDateTime() << " - TRACING [" + threadId + ":" + threadName + "] " + msg << endl;
                break;
            default:
                std::cout << getCurrentDateTime() << " - DEBUG [" + threadId + ":" + threadName + "] " + msg << endl;
        }
    };


    void mylog(int level,  std::string message)
    {


        switch(level) {
            case -2: // -2 -> fatal
                if (log_level >= -2 )
                    std::cout << getCurrentDateTime() <<" - FATAL " + message << endl;
                break;
            case -1: // -1 -> error
                if (log_level >= -1 )
                    std::cout << getCurrentDateTime() << " - ERROR " + message << endl;
                break;
            case 0: // 0 -> warning
                if (log_level >= 0 )
                    std::cout << getCurrentDateTime() << " - WARNING " + message << endl;
                break;
            case 1: // 1 -> info
                if (log_level >= 1 )
                    std::cout << getCurrentDateTime() << " - INFO " + message << endl;
                break;
            case 2: // 2 -> debug
                if (log_level >= 2 )
                    std::cout << getCurrentDateTime() << " - DEBUG " + message << endl;
                break;
            case 3: // 3 -> tracing
                if (log_level >= 3 )
                    std::cout << getCurrentDateTime() << " - TRACING " + message << endl;
                break;
            default:
                std::cout << getCurrentDateTime() << " - DEBUG " + message << endl;
        }
    };
    
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
        // 记录一条信息
        LOG(INFO) << "initialize easylogger finished.";
    }
} // namespace Utils
