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

#include "redis/Transaction.hh"
#include "utils/Macros.hh"
#include "utils/IntToBinaryString.hh"
using namespace quarkdb;

Transaction::Transaction(RedisRequest &&req) {
  this->emplace_back(std::move(req));
  checkNthCommandForWrites();
  setPhantom(true);
}

Transaction::Transaction() {}

Transaction::~Transaction() {}

void Transaction::push_back(RedisRequest &&req) {
  requests.push_back(std::move(req));
  checkNthCommandForWrites();
}

void serializeRequestToString(std::stringstream &ss, const RedisRequest &req) {
  ss << intToBinaryString(req.size());
  for(size_t i = 0; i < req.size(); i++) {
    ss << intToBinaryString(req[i].size());
    ss.write(req[i].data(), req[i].size());
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

void Transaction::checkNthCommandForWrites(int n) {
  if(n == -1) n = requests.size() - 1;
  RedisRequest &lastreq = requests[n];

  qdb_assert(lastreq.getCommandType() == CommandType::READ || lastreq.getCommandType() == CommandType::WRITE);
  if(lastreq.getCommandType() == CommandType::WRITE) {
    hasWrites = true;
  }
}

bool Transaction::deserialize(const RedisRequest &req) {
  qdb_assert(requests.empty());

  if(req.size() != 3u) return false;

  if(req.getCommand() != RedisCommand::TX_READONLY && req.getCommand() != RedisCommand::TX_READWRITE) {
    return false;
  }

  bool phantom;
  if(req[2] == "phantom") {
    phantom = true;
  }
  else if(req[2] == "real") {
    phantom = false;
  }
  else {
    return false;
  }

  if(!this->deserialize(req[1])) return false;

  if(req.getCommand() == RedisCommand::TX_READONLY) {
    qdb_assert(!this->containsWrites());
  }
  else {
    qdb_assert(this->containsWrites());
  }

  this->setPhantom(phantom);
  return true;
}

bool Transaction::deserialize(const PinnedBuffer &src) {
  qdb_assert(requests.empty());
  if(src.empty()) return false;

  const char *pos = src.data();
  int64_t totalRequests = binaryStringToInt(pos);
  pos += sizeof(int64_t);
  size_t bytesConsumed = sizeof(int64_t);

  requests.resize(totalRequests);

  for(int64_t i = 0; i < totalRequests; i++) {
    int64_t totalParts = binaryStringToInt(pos);
    pos += sizeof(int64_t);
    bytesConsumed += sizeof(int64_t);

    for(int64_t part = 0; part < totalParts; part++) {
      int64_t length = binaryStringToInt(pos);
      pos += sizeof(int64_t);
      bytesConsumed += sizeof(int64_t);

      requests[i].push_back(src.substr(bytesConsumed, length));
      pos += (length * sizeof(char));
      bytesConsumed += (length * sizeof(char));
    }
    checkNthCommandForWrites(i);
  }

  return true;
}

void Transaction::clear() {
  requests.clear();
  phantom = false;
  hasWrites = false;
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
