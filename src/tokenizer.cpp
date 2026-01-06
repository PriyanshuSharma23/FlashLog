#include "tokenizer.hpp"

auto nextToken(std::string_view &in, bool takeRest) -> std::string_view {
  while (!in.empty() && (in.front() == ' ' || in.front() == '\t'))
    in.remove_prefix(1);

  if (in.empty())
    return {};

  if (takeRest) {
    auto tok = in;
    in = {};
    return tok;
  }

  size_t i = 0;
  while (i < in.size() && in[i] != ' ' && in[i] != '\t')
    ++i;

  auto tok = in.substr(0, i);
  in.remove_prefix(i);
  return tok;
}
