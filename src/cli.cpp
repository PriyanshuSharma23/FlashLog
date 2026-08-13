#include "cli.hpp"

#include "config.hpp"
#include "logstore.hpp"
#include "slog.hpp"
#include "tokenizer.hpp"

#include <iostream>
#include <string_view>

LogStore logStore;

enum Command : std::uint8_t {
  EXIT,
  SET,
  GET,
  INVALID,
};

static Command parseCommand(std::string_view &input) {
  const auto cmd = nextToken(input);

  if (cmd == "exit")
    return Command::EXIT;
  if (cmd == "set")
    return Command::SET;
  if (cmd == "get")
    return Command::GET;
  return Command::INVALID;
}

static void handleSetCommand(std::string_view &input) {
  const auto key = std::string(nextToken(input));
  const auto value = std::string(nextToken(input, true));

  if (key.empty() || value.empty())
    throw std::runtime_error("Invalid command usage: set");

  logStore.set(key, value);
}

static void handleGetCommand(std::string_view &input) {
  const auto key = std::string(nextToken(input));

  if (key.empty())
    throw std::runtime_error("Invalid command usage: get");

  logStore.get(key);
}

void runCli() {
  LOG_TRACE << "Detected platform: " << platformName() << "\n";

  bool cliRunning = true;
  std::string userInput;

  while (cliRunning) {
    std::cout << "> ";
    std::getline(std::cin, userInput);

    std::string_view userInputView = userInput;
    Command cmd = parseCommand(userInputView);

    switch (cmd) {
    case EXIT:
      cliRunning = false;
      break;
    case SET:
      handleSetCommand(userInputView);
      break;
    case GET:
      handleGetCommand(userInputView);
      break;
    case INVALID:
      std::cout << "Unknown command: " << userInput << "\n";
      break;
    }
  }
}
