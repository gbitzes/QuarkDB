// ----------------------------------------------------------------------
// File: RedisRequest.cc
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

#include "RedisRequest.hh"
#include "redis/MultiOp.hh"
#include "utils/StringUtils.hh"
#include "utils/Macros.hh"
using namespace quarkdb;

void RedisRequest::parseCommand() {
  command = RedisCommand::INVALID;
  commandType = CommandType::INVALID;

  if(contents.size() == 0) {
    return;
  }

  auto it = redis_cmd_map.find(contents[0]);
  if(it == redis_cmd_map.end()) {
    return;
  }

  command = it->second.first;
  commandType = it->second.second;
}

std::string RedisRequest::toPrintableString() const {
  if(this->getCommand() == RedisCommand::MULTIOP_READ || this->getCommand() == RedisCommand::MULTIOP_READWRITE) {
    MultiOp multiOp;
    qdb_assert(this->size() == 3);
    qdb_assert(multiOp.deserialize((*this)[1]));

    std::stringstream ss;
    ss << (*this)[0] << " (" << (*this)[2] << "), size " << multiOp.size() << std::endl;
    for(size_t i = 0; i < multiOp.size(); i++) {
      ss << " --- " << i+1 << ") " << multiOp[i].toPrintableString();
      if(i != multiOp.size()-1) ss << std::endl;
    }

    return ss.str();
  }

  std::stringstream ss;
  for(auto it = begin(); it != end(); it++) {
    if(it != begin()) ss << " ";

    if(StringUtils::isPrintable(*it)) {
      ss << "\"" << *it << "\"";
    }
    else {
      ss << "\"" << StringUtils::escapeNonPrintable(*it) << "\"";
    }
  }
  return ss.str();
}

std::ostream& quarkdb::operator<<(std::ostream& out, const RedisRequest& req) {
  out << std::string("[");
  for(size_t i = 0; i < req.size(); i++) {
    out << std::string("'") << req[i] << std::string("'");
    if(i != req.size()-1) out << std::string(" ");
  }
  out << std::string("]");
  return out;
}
