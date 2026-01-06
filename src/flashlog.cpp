#include <filesystem>

#include "cli.hpp"
#include "slog.hpp"

auto main() -> int {
  try {
    runCli();
  } catch (const std::filesystem::filesystem_error &err) {
    LOG_FATAL << "Error: " << err.what() << "\n";
    return 1;
  } catch (const std::exception &err) {
    LOG_FATAL << "Fatal error: " << err.what() << "\n";
    return 1;
  }
}
