#include "logstore.hpp"
#include "slog.hpp"
#include "tokenizer.hpp"

#include <cassert>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string_view>

LogStore::LogStore() {}

void LogStore::snapshotIndex() {
  std::error_code ec;
  const auto currentLogFileSize = std::filesystem::file_size(LOG_FILE, ec);
  if (ec) {
    throw std::filesystem::filesystem_error("Failed to get log file size",
                                            LOG_FILE, ec);
  }

  if (currentLogFileSize - lastIndexSnapshotRefresh <
      INDEX_SNAPSHOT_REFRESH_THRESHOLD) {
    LOG_TRACE << "Snapshotindex threshold not reached, difference: "
              << currentLogFileSize - lastIndexSnapshotRefresh << "\n";
    return;
  }

  auto tempIndexFilePath = INDEX_FILE;
  tempIndexFilePath += ".tmp";

  std::ofstream tempIndexFile(tempIndexFilePath,
                              std::ios::trunc | std::ios::binary);

  lastIndexSnapshotRefresh = currentLogFileSize;

  for (const auto &[key, index] : segmentIndex) {
    tempIndexFile << key << " " << index.byteOffset << "\n";
  }

  tempIndexFile.flush();
  tempIndexFile.close();

  std::filesystem::rename(tempIndexFilePath, INDEX_FILE);

  LOG_TRACE << "Index snapshot created\n";
}

void LogStore::set(const std::string &key, const std::string &value) {
  LOG_TRACE << "Set command: " << key << " = " << value << "\n";
  LOG_TRACE << "Log file path: " << LOG_FILE << "\n";

  std::ofstream logFile(LOG_FILE, std::ios::app | std::ios::binary);
  if (!logFile)
    throw std::runtime_error("open failed");

  const auto offset = std::filesystem::file_size(LOG_FILE);
  std::string log = "SET " + key + " " + value + "\n";

  logFile.write(log.c_str(), (long long)log.size());
  logFile.flush();

  segmentIndex.insert_or_assign(key, Index{offset, key});
  LOG_TRACE << "Offset: " << offset << "\n";

  snapshotIndex();
}

std::string LogStore::get(const std::string &key) {
  LOG_TRACE << "Get command: " << key << "\n";

  std::ifstream logFile(LOG_FILE, std::ios::binary);

  if (!logFile)
    throw std::runtime_error("open failed");

  auto indexEntry = segmentIndex.find(key);
  if (indexEntry == segmentIndex.end()) {
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
