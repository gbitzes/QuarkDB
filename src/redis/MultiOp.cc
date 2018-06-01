// ----------------------------------------------------------------------
// File: MultiOp.cc
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

#include "MultiOp.hh"
#include "../utils/Macros.hh"
#include "../utils/IntToBinaryString.hh"
using namespace quarkdb;

MultiOp::MultiOp() {}

MultiOp::~MultiOp() {}

void MultiOp::push_back(const RedisRequest &req) {
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

std::string MultiOp::serialize() const {
  std::stringstream ss;
  ss << intToBinaryString(requests.size());

  for(size_t i = 0; i < requests.size(); i++) {
    serializeRequestToString(ss, requests[i]);
  }

  return ss.str();
}

void MultiOp::checkLastCommandForWrites() {
  RedisRequest &lastreq = requests.back();

  qdb_assert(lastreq.getCommandType() == CommandType::READ || lastreq.getCommandType() == CommandType::WRITE);
  if(lastreq.getCommandType() == CommandType::WRITE) {
    hasWrites = true;
  }
}

bool MultiOp::deserialize(const std::string &src) {
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

void MultiOp::clear() {
  requests.clear();
}

std::string MultiOp::getFusedCommand() const {
  if(hasWrites) {
    return "MULTIOP_READWRITE";
  }

  return "MULTIOP_READ";
}

RedisRequest MultiOp::toRedisRequest() const {
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
