#pragma once

#include <string_view>

std::string_view nextToken(std::string_view &in, bool takeRest = false);
