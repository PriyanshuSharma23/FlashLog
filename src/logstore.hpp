#pragma once

#include "config.hpp"
#include "index.hpp"

#include <filesystem>
#include <string>
#include <unordered_map>

// This class is responsible for reading and writing into the log file and
// maintaing index
class LogStore {
public:
  explicit LogStore();

  void set(const std::string &key, const std::string &value);
  std::string get(const std::string &key);

protected:
  void loadIndex();
  void snapshotIndex();
  void appendLog();

private:
  std::unordered_map<std::string, Index> segmentIndex;
  uint64_t lastIndexSnapshotRefresh = 0;
  const uint64_t INDEX_SNAPSHOT_REFRESH_THRESHOLD =
      static_cast<const uint64_t>(256);

  Config cfg = getConfig();
  std::filesystem::path SEGMENTS_DIR = cfg.data / "segments";
  std::filesystem::path LOG_FILE = SEGMENTS_DIR / "log.txt";
  std::filesystem::path INDEX_FILE = SEGMENTS_DIR / "index.txt";
};
