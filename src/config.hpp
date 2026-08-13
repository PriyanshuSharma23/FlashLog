#pragma once
#include <filesystem>

struct Config {
  std::filesystem::path config;
  std::filesystem::path data;
};

auto getConfig() -> Config;
auto platformName() -> const char *;
