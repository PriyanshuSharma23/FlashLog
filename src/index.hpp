#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

struct Index {
  uint64_t byteOffset;
  std::string key;

  Index(uint64_t o, std::string k) : byteOffset(o), key(std::move(k)) {}
};

using IndexMap = std::unordered_map<std::string, Index>;
