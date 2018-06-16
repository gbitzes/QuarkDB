// ----------------------------------------------------------------------
// File: Transaction.cc
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

#include "Transaction.hh"
#include "../utils/Macros.hh"
#include "../utils/IntToBinaryString.hh"
using namespace quarkdb;

Transaction::Transaction() {}

Transaction::~Transaction() {}

void Transaction::push_back(const RedisRequest &req) {
  requests.push_back(req);
  checkLastCommandForWrites();
}

void serializeRequestToString(std::stringstream &ss, const RedisRequest &req) {
  ss << intToBinaryString(req.size());
  for(size_t i = 0; i < req.size(); i++) {
    ss << intToBinaryString(req[i].size());
    ss.write(req[i].c_str(), req[i].size());
  }
}

std::string Transaction::serialize() const {
  std::stringstream ss;
  ss << intToBinaryString(requests.size());

  for(size_t i = 0; i < requests.size(); i++) {
    serializeRequestToString(ss, requests[i]);
  }

  return ss.str();
}

void Transaction::checkLastCommandForWrites() {
  RedisRequest &lastreq = requests.back();

  qdb_assert(lastreq.getCommandType() == CommandType::READ || lastreq.getCommandType() == CommandType::WRITE);
  if(lastreq.getCommandType() == CommandType::WRITE) {
    hasWrites = true;
  }
}

bool Transaction::deserialize(const std::string &src) {
  qdb_assert(requests.empty());
  if(src.empty()) return false;

  const char *pos = src.c_str();
  int64_t totalRequests = binaryStringToInt(pos);
  pos += sizeof(int64_t);

  for(int64_t i = 0; i < totalRequests; i++) {
    requests.emplace_back();

    int64_t totalParts = binaryStringToInt(pos);
    pos += sizeof(int64_t);

    for(int64_t part = 0; part < totalParts; part++) {
      int64_t length = binaryStringToInt(pos);
      pos += sizeof(int64_t);

      requests[i].push_back(std::string(pos, length));
      pos += (length * sizeof(char));
    }
    checkLastCommandForWrites();
  }

  return true;
}

void Transaction::clear() {
  requests.clear();
}

std::string Transaction::getFusedCommand() const {
  if(hasWrites) {
    return "TX_READWRITE";
  }

  return "TX_READONLY";
}

RedisRequest Transaction::toRedisRequest() const {
  if(phantom && requests.size() == 1) {
    return requests[0];
  }

  RedisRequest req;
  req.emplace_back(getFusedCommand());
  req.emplace_back(serialize());

  if(phantom) {
    req.emplace_back("phantom");
  }
  else {
    req.emplace_back("real");
  }

  return req;
}

void Transaction::fromRedisRequest(const RedisRequest &req) {
  qdb_assert(req.getCommand() == RedisCommand::TX_READONLY || req.getCommand() == RedisCommand::TX_READWRITE);
  qdb_assert(req.size() == 3);
  qdb_assert(deserialize(req[1]));

  if(req[2] == "phantom") {
    setPhantom(true);
  }
  else if(req[2] == "real") {
    setPhantom(false);
  }
  else {
    qdb_throw("should never happen");
  }

}

std::string Transaction::typeInString() const {
  if(phantom) {
    return "phantom";
  }
  return "real";
}

std::string Transaction::toPrintableString() const {
  std::stringstream ss;
  ss << getFusedCommand() << " (" << typeInString() << "), size " << requests.size() << std::endl;
  for(size_t i = 0; i < requests.size(); i++) {
    ss << " --- " << i+1 << ") " << requests[i].toPrintableString();
    if(i != requests.size()-1) ss << std::endl;
  }

  return ss.str();
}
