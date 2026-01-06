#pragma once
#include <filesystem>

struct Config {
  std::filesystem::path config;
  std::filesystem::path data;
};

Config getConfig();
const char *platformName();
