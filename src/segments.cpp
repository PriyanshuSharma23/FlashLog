#include "segments.hpp"
#include "slog.hpp"
#include "tokenizer.hpp"

#include <filesystem>
#include <fstream>
#include <ostream>
#include <set>
#include <unordered_map>

static void createDirectory(const std::filesystem::path &p) {
  std::error_code ec;
  if (std::filesystem::create_directory(p, ec)) {
    LOG_TRACE << "Directory created: " << p << "\n";
  } else if (ec) {
    throw std::filesystem::filesystem_error("Failed to create directory", p,
                                            ec);
  }
}

void Segments::initializeDirectories() {
  if (!std::filesystem::exists(cfg.data) ||
      !std::filesystem::is_directory(cfg.data)) {
    createDirectory(cfg.data);
  }

  if (!std::filesystem::exists(cfg.config) ||
      !std::filesystem::is_directory(cfg.config)) {
    createDirectory(cfg.config);
  }

  if (!std::filesystem::exists(cfg.data / Segment::SEGMENT_DIR_NAME) ||
      !std::filesystem::is_directory(cfg.data / Segment::SEGMENT_DIR_NAME)) {
    createDirectory(cfg.data / Segment::SEGMENT_DIR_NAME);
  }
}

IndexMap buildIndexFromSnapshot(std::filesystem::path &path) {
  std::error_code ec;
  IndexMap index;

  std::ifstream snapshotFile(path, std::ios::binary);

  if (snapshotFile.is_open()) {
    std::string line;

    while (std::getline(snapshotFile, line)) {
      auto lineView = std::string_view(line);
      auto key = std::string(nextToken(lineView));

      if (key.size() == 0) {
        throw std::runtime_error("Invalid snapshot file");
      }

      auto offsetStr = std::string(nextToken(lineView, true));
      if (offsetStr.size() == 0) {
        throw std::runtime_error("Invalid snapshot file");
      }
      auto offset = std::stoull(offsetStr);

      index[key] = Index(offset, key);
    }

    snapshotFile.close();
  } else {
    throw std::filesystem::filesystem_error("Failed to open snapshot file",
                                            path, ec);
  }

  return index;
}

void Segments::initializeSegments() {
  this->segments = std::map<size_t, Segment>();

  std::set<size_t> indexFileIds;

  for (auto &e : std::filesystem::directory_iterator(
           cfg.data / Segment::SEGMENT_DIR_NAME)) {
    if (e.is_regular_file() &&
        e.path().extension() == Segment::SEGMENT_INDEX_FILE_EXTENSION) {
      auto indexFileId = std::stoul(e.path().stem().string());
      indexFileIds.insert(indexFileId);
    }
  }

  for (auto &e : std::filesystem::directory_iterator(
           cfg.data / Segment::SEGMENT_DIR_NAME)) {

    if (e.is_regular_file() &&
        e.path().extension() == Segment::SEGMENT_FILE_EXTENSION) {
      auto logFileId = std::stoul(e.path().stem().string());

      std::unordered_map<std::string, Index> *index = nullptr;
      if (indexFileIds.find(logFileId) == indexFileIds.end()) {
        LOG_WARN << "Index not found in log file id: " << logFileId << "\n";
      } else {
      }

      this->segments[logFileId] = Segment(logFileId, *index);
    }
  }
}

Segments::Segments() {
  initializeDirectories();
  initializeSegments();
}

auto Segments::maxSegmentSize() -> uint64_t {
  return static_cast<const uint64_t>(32 * 1024 * 1024); // 32MB
}

Segment &Segments::getCurrentSegment() {
  if (segments.find(currentSegmentId) == segments.end()) {
    throw std::runtime_error("No more segments available");
  }

  return segments[currentSegmentId];
}

void Segments::snapshotIndex() {
  auto &currentSegment = getCurrentSegment();

  std::error_code ec;
  const auto currentLogFileSize =
      std::filesystem::file_size(currentSegment.getLogFilePath(), ec);
  if (ec) {
    throw std::filesystem::filesystem_error(
        "Failed to get log file size", currentSegment.getLogFilePath(), ec);
  }

  if (currentLogFileSize - lastIndexSnapshotRefresh <
      INDEX_SNAPSHOT_REFRESH_THRESHOLD) {
    LOG_TRACE << "Snapshotindex threshold not reached, difference: "
              << currentLogFileSize - lastIndexSnapshotRefresh << "\n";
    return;
  }

  auto tempIndexFilePath = currentSegment.getIndexFilePath();
  tempIndexFilePath += ".tmp";

  std::ofstream tempIndexFile(tempIndexFilePath,
                              std::ios::trunc | std::ios::binary);

  lastIndexSnapshotRefresh = currentLogFileSize;

  for (const auto &[key, index] : currentSegment.index) {
    tempIndexFile << key << " " << index.byteOffset << "\n";
  }

  tempIndexFile.flush();
  tempIndexFile.close();

  std::filesystem::rename(tempIndexFilePath, currentSegment.getIndexFilePath());

  LOG_TRACE << "Index snapshot created\n";
}
