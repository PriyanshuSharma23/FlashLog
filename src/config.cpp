#include "config.hpp"
#include <cstdlib>

auto getConfig() -> Config {
#ifdef _WIN32
  auto base = std::filesystem::path(getenv("APPDATA")) / "FlashLog";
#else
  auto base = std::filesystem::path(getenv("HOME")) / ".local/share/FlashLog";
#endif
  return {.config = base, .data = base};
}

auto platformName() -> const char * {
#ifdef _WIN32
  return "windows";
#else
  return "unix";
#endif
}
