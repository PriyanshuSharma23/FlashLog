#pragma once
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>

namespace slog {

enum class Level { Trace, Info, Warn, Error, Fatal };

inline const char *level_name(Level l) {
  switch (l) {
  case Level::Trace:
    return "TRACE";
  case Level::Info:
    return "INFO";
  case Level::Warn:
    return "WARN";
  case Level::Error:
    return "ERROR";
  case Level::Fatal:
    return "FATAL";
  }
  return "UNKNOWN";
}

inline const char *level_color(Level l) {
  switch (l) {
  case Level::Trace:
    return "\033[90m";
  case Level::Info:
    return "\033[36m";
  case Level::Warn:
    return "\033[33m";
  case Level::Error:
    return "\033[31m";
  case Level::Fatal:
    return "\033[41m";
  }
  return "\033[0m";
}

inline std::string timestamp() {
  using namespace std::chrono;
  auto t = system_clock::to_time_t(system_clock::now());
  std::tm tm{};
#ifdef _WIN32
  localtime_s(&tm, &t);
#else
  localtime_r(&t, &tm);
#endif
  std::ostringstream oss;
  oss << std::put_time(&tm, "%H:%M:%S");
  return oss.str();
}

inline void commit(Level lvl, const std::string &msg) {
  static std::mutex mtx;
  std::lock_guard lock(mtx);

  std::cout << level_color(lvl) << "[" << timestamp() << "] "
            << "[" << level_name(lvl) << "] " << msg << "\033[0m" << std::endl;

  if (lvl == Level::Fatal)
    std::terminate();
}

class Stream {
public:
  explicit Stream(Level lvl) : lvl(lvl) {}
  ~Stream() { commit(lvl, ss.str()); }

  template <typename T> Stream &operator<<(const T &v) {
    ss << v;
    return *this;
  }

private:
  Level lvl;
  std::ostringstream ss;
};

// Macro entry points
#define LOG_TRACE ::slog::Stream(::slog::Level::Trace)
#define LOG_INFO ::slog::Stream(::slog::Level::Info)
#define LOG_WARN ::slog::Stream(::slog::Level::Warn)
#define LOG_ERROR ::slog::Stream(::slog::Level::Error)
#define LOG_FATAL ::slog::Stream(::slog::Level::Fatal)

} // namespace slog
