#include "config.hpp"
#include <cstdlib>

Config getConfig() {
#ifdef _WIN32
  auto base = std::filesystem::path(getenv("APPDATA")) / "FlashLog";
#else
  auto base = std::filesystem::path(getenv("HOME")) / ".local/share/FlashLog";
#endif
  return {base, base};
}

const char *platformName() {
#ifdef _WIN32
  return "windows";
#else
  return "unix";
#endif
}
