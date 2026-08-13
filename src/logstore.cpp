#include "logstore.hpp"
#include "index.hpp"
#include "slog.hpp"
#include "tokenizer.hpp"

#include <cassert>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string_view>
#include <unordered_map>

LogStore::LogStore() = default;

void LogStore::set(const std::string &key, const std::string &value) {
  auto &segment = segments.getCurrentSegment();
  LOG_TRACE << "Set command: " << key << " = " << value << "\n";
  LOG_TRACE << "Log file path: " << segment.getLogFilePath() << "\n";

  std::ofstream logFile(segment.getLogFilePath(),
                        std::ios::app | std::ios::binary);
  if (!logFile)
    throw std::runtime_error("open failed");

  const auto offset = std::filesystem::file_size(segment.getLogFilePath());
  std::string log = "SET " + key + " " + value + "\n";

  logFile.write(log.c_str(), (long long)log.size());
  logFile.flush();

  segment.index.try_emplace(key, offset, key);
  LOG_TRACE << "Offset: " << offset << "\n";

  segments.snapshotIndex();
}

auto LogStore::get(const std::string &key) -> std::string {
  auto &segment = segments.getCurrentSegment();
  LOG_TRACE << "Get command: " << key << "\n";

  std::ifstream logFile(segment.getLogFilePath(), std::ios::binary);

  if (!logFile)
    throw std::runtime_error("open failed");

  auto indexEntry = segment.index.find(key);
  if (indexEntry == segment.index.end()) {
    throw std::runtime_error("Key not found");
  }

  auto &index = indexEntry->second;
  LOG_TRACE << "Offset: " << index.byteOffset << "\n";

  logFile.seekg((long long)index.byteOffset);

  std::string log;
  std::getline(logFile, log);

  auto log_view = std::string_view(log);

  assert(nextToken(log_view) == "SET");
  assert(nextToken(log_view) == key);

  auto value = std::string(nextToken(log_view, true));

  LOG_TRACE << "Value: " << value << "\n";

  return value;
}
