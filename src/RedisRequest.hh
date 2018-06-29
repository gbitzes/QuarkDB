// ----------------------------------------------------------------------
// File: RedisRequest.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2016 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#ifndef QUARKDB_REDIS_REQUEST_H
#define QUARKDB_REDIS_REQUEST_H

#include "Commands.hh"
#include <string>
#include <vector>
#include <sstream>

namespace quarkdb {

class RedisRequest {
public:
  using container = std::vector<std::string>;
  using iterator = container::iterator;
  using const_iterator = container::const_iterator;
  using size_type = container::size_type;

  RedisRequest(std::initializer_list<std::string> list) {
    for(auto it = list.begin(); it != list.end(); it++) {
      contents.push_back(*it);
    }
    parseCommand();
  }

  RedisRequest() {}

  size_t size() const {
    return contents.size();
  }

  std::string&& move(size_t i) {
    invalidateCommand();
    return std::move(contents[i]);
  }

  std::string& operator[](size_t i) {
    return contents[i];
  }

  const std::string& operator[](size_t i) const {
    return contents[i];
  }

  bool operator==(const RedisRequest &rhs) const {
    return contents == rhs.contents;
  }

  bool operator!=(const RedisRequest &rhs) const {
    return !(contents == rhs.contents);
  }

  void clear() {
    invalidateCommand();
    contents.clear();
  }

  void push_back(const std::string &str) {
    contents.emplace_back(str);
    if(contents.size() == 1) parseCommand();
  }

  void emplace_back(std::string &&src) {
    contents.emplace_back(std::move(src));
    if(contents.size() == 1) parseCommand();
  }

  void emplace_back(const char* buf, size_t size) {
    contents.emplace_back(buf, size);
    if(contents.size() == 1) parseCommand();
  }

  const_iterator begin() const {
    return contents.begin();
  }

  const_iterator end() const {
    return contents.end();
  }

  void reserve(size_t size) {
    contents.reserve(size);
  }

  RedisCommand getCommand() const {
    return command;
  }

  CommandType getCommandType() const {
    return commandType;
  }

  void resize(size_t n) {
    contents.resize(n);
  }

  void parseCommand();
  std::string toPrintableString() const;

  void invalidate() {
    command = RedisCommand::INVALID;
    commandType = CommandType::INVALID;
  }

private:
  std::vector<std::string> contents;
  RedisCommand command = RedisCommand::INVALID;
  CommandType commandType = CommandType::INVALID;

  void invalidateCommand() {
    command = RedisCommand::INVALID;
    commandType = CommandType::INVALID;
  }

};

std::ostream& operator<<(std::ostream& out, const RedisRequest& req);

}

#endif
