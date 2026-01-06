#include "config.hpp"
#include "logstore.hpp"
#include "slog.hpp"
#include "tokenizer.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string_view>
#include <unordered_map>

const uint64_t MAX_SEGMENT_SIZE =
    static_cast<const uint64_t>(32 * 1024 * 1024); // 32MB

Config cfg = getConfig();
std::filesystem::path DATA_PATH_DIR = cfg.data;
std::filesystem::path SEGMENTS_DIR = DATA_PATH_DIR / "segments";
std::filesystem::path LOG_FILE = SEGMENTS_DIR / "log.txt";
std::filesystem::path INDEX_FILE = SEGMENTS_DIR / "index.txt";
std::unordered_map<std::string, Index> segmentIndex;
uint64_t lastIndexSnapshotRefresh = 0;

LogStore logStore;

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

void handleSetCommand(std::string_view &input) {
  const auto key = std::string(nextToken(input));
  const auto value = std::string(nextToken(input, true));

  if (key.empty() || value.empty())
    throw std::runtime_error("Invalid command usage: set");

  logStore.set(key, value);
}

void handleGetCommand(std::string_view &input) {
  const auto key = std::string(nextToken(input));

  if (key.empty())
    throw std::runtime_error("Invalid command usage: get");

  logStore.get(key);
}

auto main() -> int {
  try {
    LOG_TRACE << "Detected platform: " << platformName() << "\n";

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
