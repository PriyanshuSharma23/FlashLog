#include "slog.hpp"
#include <filesystem>
#include <iostream>
#include <string_view>
#include <unordered_map>

#ifdef _WIN32
std::filesystem::path CONFIG_DIR =
    std::filesystem::fs::path(getenv("APPDATA")) / "FlashLog";
std::filesystem::path DATA_PATH_DIR =
    std::filesystem::fs::path(getenv("APPDATA")) / "FlashLog";
char PLATFORM[] = "windows";
#else
std::filesystem::path CONFIG_DIR =
    std::filesystem::path(getenv("HOME")) / ".config" / "FlashLog";
std::filesystem::path DATA_PATH_DIR =
    std::filesystem::path(getenv("HOME")) / ".local" / "share" / "FlashLog";
char PLATFORM[] = "unix";
#endif

std::filesystem::path SEGMENTS_DIR = DATA_PATH_DIR / "segments";

struct Index {
  uint64_t byteOffset;
  std::string key;
};

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

std::string_view nextToken(std::string_view &input) {
  input.remove_prefix(std::min(input.find_first_not_of(" \t"), input.size()));
  const auto end = input.find_first_of(" \t");

  const auto tok = input.substr(0, end);
  input.remove_prefix(std::min(end, input.size()));
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

void setCommand(const std::string &key, const std::string &value) {
  LOG_TRACE << "Set command: " << key << " = " << value << "\n";
}

void getCommand(const std::string &key) {
  LOG_TRACE << "Get command: " << key << "\n";
}

void handleSetCommand(std::string_view &input) {
  const auto key = std::string(nextToken(input));
  const auto value = std::string(nextToken(input));

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

    std::unordered_map<std::string, Index> segmentIndex;

    bool cliRunnig = true;
    std::string userInput;

    while (cliRunnig) {
      std::cout << "> ";
      std::getline(std::cin, userInput);

      std::string_view userInputView = userInput;
      Command cmd = parseCommand(userInputView);

      switch (cmd) {
      case EXIT:
        cliRunnig = false;
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
