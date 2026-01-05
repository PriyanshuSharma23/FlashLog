#include "slog.hpp"
#include <filesystem>
#include <iostream>

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

void loadSegments() {}

int main(int argc, char **argv) {
  LOG_TRACE << "Detected platform: " << PLATFORM << "\n";

  // TODO: Load config

  initializeDataDir();

  bool cliRunnig = true;
  std::string userInput;

  while (cliRunnig) {
    std::cout << "> ";
    std::getline(std::cin, userInput);

    if (userInput == "exit") {
      cliRunnig = false;
    } else {
      std::cout << "Unknown command: " << userInput << "\n";
    }
  }
}
