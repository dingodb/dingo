// utils.cpp
#include <utils.h>
#include <set>
#include <random>
#include <stdexcept>
#include <algorithm>
#include <locale>
#include <codecvt>
#include <chrono>
#include <iomanip>

INITIALIZE_EASYLOGGINGPP

namespace Utils {

    int log_level = 0;

    std::string getCurrentDateTime() {
        auto now = std::chrono::system_clock::now(); // get current time.
        auto now_converted = std::chrono::system_clock::to_time_t(now); // convert to `time_t`
        std::ostringstream oss;
        oss << std::put_time(std::localtime(&now_converted), "%Y-%m-%d %H:%M:%S");
        return oss.str();
    }

    void tantivy_easylogging_callback(int level, const char* thread_info, const char* message)
    {
        std::string threadInfo(thread_info);
        std::string msg(message);

        switch(level) {
            case -2: // -2 -> fatal
                if (log_level >= -2 )
                    LOG(FATAL) << threadInfo << " - " <<msg;
                break;
            case -1: // -1 -> error
                if (log_level >= -1 )
                    LOG(ERROR) << threadInfo << " - " <<msg;
                break;
            case 0: // 0 -> warning
                if (log_level >= 0 )
                    LOG(WARNING) << threadInfo << " - " <<msg;
                break;
            case 1: // 1 -> info
                if (log_level >= 1 )
                    LOG(INFO) << threadInfo << " - " <<msg;
                break;
            case 2: // 2 -> debug
                if (log_level >= 2 )
                    LOG(DEBUG) << threadInfo << " - " <<msg;
                break;
            case 3: // 3 -> tracing
                if (log_level >= 3 )
                    LOG(TRACE) << threadInfo << " - " <<msg;
                break;
            default:
                LOG(DEBUG) << threadInfo << " - " <<msg;
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
        defaultConf.set(log_level, el::ConfigurationType::Enabled, "true");
        // TODO: need diable debug, trace manually.
        defaultConf.set(log_level, el::ConfigurationType::Format, "%datetime %level %msg"); // formatter
        defaultConf.set(log_level, el::ConfigurationType::ToFile, "true"); // write to file
        defaultConf.set(log_level, el::ConfigurationType::ToStandardOutput, "true"); // std out
        el::Loggers::reconfigureLogger("default", defaultConf); // apply config to logger
        LOG(INFO) << "Easylogger has been initialized.";
    }

    std::string BoundaryUnitTestUtils::generateRandomStringFromCharacters(size_t length, std::wstring characters) {
        std::random_device random_device;
        std::mt19937 generator(random_device());
        std::uniform_int_distribution<> distribution(0, characters.size() - 1);

        std::wstring random_string;
        for (size_t i = 0; i < length; ++i) {
            if (i%5==0)
            {
                random_string += ' ';
            }
            random_string += characters[distribution(generator)];
        }

        std::wstring_convert<std::codecvt_utf8<wchar_t>> converter;
        return converter.to_bytes(random_string);
    }
    std::string BoundaryUnitTestUtils::generateRandomString(size_t length) {
        // std::string not work friendly with utf-8
        const std::wstring characters = L"ab, cdefg? hijkl> mn. opqr 🚀 stuvw, xyzA! BCDEF(12) \
                                    GHIJK[l] 天气 不错 PQRS TUV どこでじゃがいもを掘るか、一掘りで一袋";
        return generateRandomStringFromCharacters(length, characters);
    }

    std::string BoundaryUnitTestUtils::generateRandomNormalString(size_t length) {
        // std::string not work friendly with utf-8
        const std::wstring characters = L"a bc def gA BCD EFGど こじ ゃが いもを掘るか  一掘りで一袋 摸鱼 🐟";
        return generateRandomStringFromCharacters(length, characters);
    }

    std::vector<uint8_t> BoundaryUnitTestUtils::generateRandomUInt8Vector(size_t length) {
            std::vector<uint8_t> u8Array(length);
            // Create a random device and use it to seed the Mersenne Twister engine.
            std::random_device rd;
            std::mt19937 gen(rd());
            // Define the distribution range for uint8_t: 0 to 255
            std::uniform_int_distribution<uint8_t> dis(0, 255);
            // Fill the vector with random uint8_t values.
            for (size_t i = 0; i < length; ++i) {
                u8Array[i] = dis(gen);
            }
            return u8Array;
        }
} // namespace Utils
