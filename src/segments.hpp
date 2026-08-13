#pragma once

#include "config.hpp"
#include "index.hpp"

#include <cstdint>
#include <filesystem>
#include <map>
#include <unordered_map>
#include <utility>

struct Segment {
  inline static constexpr const char SEGMENT_FILE_EXTENSION[] = ".log";
  inline static constexpr const char SEGMENT_INDEX_FILE_EXTENSION[] = ".index";
  inline static constexpr const char SEGMENT_DIR_NAME[] = "segments";

  size_t id;
  std::unordered_map<std::string, Index> index;

  Segment(size_t id, std::unordered_map<std::string, Index> index)
      : id(id), index(std::move(index)) {}

  auto getLogFilePath() const -> std::filesystem::path {
    return cfg.data / SEGMENT_DIR_NAME /
           (std::to_string(id) + SEGMENT_FILE_EXTENSION);
  };

  auto getIndexFilePath() const -> std::filesystem::path {
    return cfg.data / SEGMENT_DIR_NAME /
           (std::to_string(id) + SEGMENT_INDEX_FILE_EXTENSION);
  };

private:
  Config cfg = getConfig();
};

// This class is responsible for managing the storage
class Segments {
public:
  explicit Segments();
  Segment &getCurrentSegment();
  void refreshSegment();
  void snapshotIndex();
  static auto maxSegmentSize() -> uint64_t;

private:
  Config cfg = getConfig();
  std::map<size_t, Segment> segments;
  size_t currentSegmentId;
  uint64_t lastIndexSnapshotRefresh = 0;
  const uint64_t INDEX_SNAPSHOT_REFRESH_THRESHOLD =
      static_cast<const uint64_t>(256);
  void initializeSegments();
  void initializeDirectories();
  void newSegment();

  size_t maxId = INT_MAX; // need to initialize this
  size_t getNextSegmentId();
};
