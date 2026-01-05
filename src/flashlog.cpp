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
  } else {
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
  const auto first = input.find_first_not_of(" \t");
  if (first == std::string_view::npos)
    return Command::INVALID;

  input.remove_prefix(first);

  const auto end = input.find_first_of(" \t");
  std::string_view cmd = input.substr(0, end);

  if (end == std::string_view::npos) {
    input = {};
  } else {
    input.remove_prefix(end);
  }

  if (cmd == "exit")
    return Command::EXIT;
  if (cmd == "set")
    return Command::SET;
  if (cmd == "get")
    return Command::GET;
  return Command::INVALID;
}

int main(int argc, char **argv) {
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
      std::cout << "Set command\n";
      break;
    case GET:
      std::cout << "Get command\n";
      break;
    case INVALID:
      std::cout << "Unknown command: " << userInput << "\n";
      break;
    }

    LOG_TRACE << "Command after parseCommand: " << userInputView << "\n";
  }
}
