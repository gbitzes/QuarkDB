// ----------------------------------------------------------------------
// File: RaftUtils.hh
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

#ifndef __QUARKDB_RAFT_PARSER_H__
#define __QUARKDB_RAFT_PARSER_H__

#include "../Tunnel.hh"
#include "RaftCommon.hh"
#include "../Common.hh"
#include "RaftState.hh"
#include "RaftTimeouts.hh"

namespace quarkdb {

class RaftParser {
public:
  static bool appendEntries(RedisRequest &&source, RaftAppendEntriesRequest &dest);
  static bool appendEntriesResponse(const redisReplyPtr &source, RaftAppendEntriesResponse &dest);
};

}

#endif
