#pragma once

#include "config.hpp"
#include "segments.hpp"

#include <filesystem>
#include <string>

// This class is responsible for reading and writing into the log file and
// maintaing index
class LogStore {
public:
  explicit LogStore();

  void set(const std::string &key, const std::string &value);
  std::string get(const std::string &key);

protected:
  void loadIndex();
  void appendLog();

private:
  Config cfg = getConfig();
  Segments segments;
};
