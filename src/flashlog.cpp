#include "slog.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string_view>
#include <unordered_map>

// TODO: Move initialization to a funciton
#ifdef _WIN32
std::filesystem::path CONFIG_DIR =
    std::filesystem::path(getenv("APPDATA")) / "FlashLog";
std::filesystem::path DATA_PATH_DIR =
    std::filesystem::path(getenv("APPDATA")) / "FlashLog";
char PLATFORM[] = "windows";
#else
std::filesystem::path CONFIG_DIR =
    std::filesystem::path(getenv("HOME")) / ".config" / "FlashLog";
std::filesystem::path DATA_PATH_DIR =
    std::filesystem::path(getenv("HOME")) / ".local" / "share" / "FlashLog";
char PLATFORM[] = "unix";
#endif

const uint64_t MAX_SEGMENT_SIZE =
    static_cast<const uint64_t>(32 * 1024 * 1024); // 32MB
const uint64_t INDEX_SNAPSHOT_REFRESH_THRESHOLD =
    static_cast<const uint64_t>(256); // 1MB

struct Index {
  uint64_t byteOffset;
  std::string key;
};

std::filesystem::path SEGMENTS_DIR = DATA_PATH_DIR / "segments";
std::filesystem::path LOG_FILE = SEGMENTS_DIR / "log.txt";
std::filesystem::path INDEX_FILE = SEGMENTS_DIR / "index.txt";
std::unordered_map<std::string, Index> segmentIndex;
uint64_t lastIndexSnapshotRefresh = 0;

enum Command : std::uint8_t {
  EXIT,
  SET,
  GET,
  INVALID,
};

void createDirectory(const std::filesystem::path &p) {
  std::error_code ec;
  if (std::filesystem::create_directory(p, ec)) {
    LOG_TRACE << "Directory created: " << p << "\n";
  } else if (ec) {
    throw std::filesystem::filesystem_error("Failed to create directory", p,
                                            ec);
  }
}

void initializeDataDir() {
  if (!std::filesystem::exists(DATA_PATH_DIR) ||
      !std::filesystem::is_directory(DATA_PATH_DIR)) {
    createDirectory(DATA_PATH_DIR);
  }

  if (!std::filesystem::exists(SEGMENTS_DIR) ||
      !std::filesystem::is_directory(SEGMENTS_DIR)) {
    createDirectory(SEGMENTS_DIR);
  }
}

std::string_view nextToken(std::string_view &in, bool takeRest = false) {
  while (!in.empty() && (in.front() == ' ' || in.front() == '\t'))
    in.remove_prefix(1);

  if (in.empty())
    return {};

  if (takeRest) {
    auto tok = in;
    in = {};
    return tok;
  }

  size_t i = 0;
  while (i < in.size() && in[i] != ' ' && in[i] != '\t')
    ++i;

  auto tok = in.substr(0, i);
  in.remove_prefix(i);
  return tok;
}

Command parseCommand(std::string_view &input) {
  const auto cmd = nextToken(input);

  if (cmd == "exit")
    return Command::EXIT;
  if (cmd == "set")
    return Command::SET;
  if (cmd == "get")
    return Command::GET;
  return Command::INVALID;
}

void snapshotIndex() {
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

void setCommand(const std::string &key, const std::string &value) {
  LOG_TRACE << "Set command: " << key << " = " << value << "\n";

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

std::string getCommand(const std::string &key) {
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

void handleSetCommand(std::string_view &input) {
  const auto key = std::string(nextToken(input));
  const auto value = std::string(nextToken(input, true));

  if (key.empty() || value.empty())
    throw std::runtime_error("Invalid command usage: set");

  setCommand(key, value);
}

void handleGetCommand(std::string_view &input) {
  const auto key = std::string(nextToken(input));

  if (key.empty())
    throw std::runtime_error("Invalid command usage: get");

  getCommand(key);
}

auto main() -> int {
  try {
    LOG_TRACE << "Detected platform: " << PLATFORM << "\n";

    // TODO: Load config

    initializeDataDir();

    bool cliRunning = true;
    std::string userInput;

    while (cliRunning) {
      std::cout << "> ";
      std::getline(std::cin, userInput);

      std::string_view userInputView = userInput;
      Command cmd = parseCommand(userInputView);

      switch (cmd) {
      case EXIT:
        cliRunning = false;
        break;
      case SET:
        handleSetCommand(userInputView);
        break;
      case GET:
        handleGetCommand(userInputView);
        break;
      case INVALID:
        std::cout << "Unknown command: " << userInput << "\n";
        break;
      }
    }
  } catch (const std::filesystem::filesystem_error &err) {
    LOG_FATAL << "Error: " << err.what() << "\n";
    return 1;
  } catch (const std::exception &err) {
    LOG_FATAL << "Fatal error: " << err.what() << "\n";
    return 1;
  }
}
