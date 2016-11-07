// ----------------------------------------------------------------------
// File: RaftTalker.hh
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

#ifndef __QUARKDB_RAFT_TALKER_H__
#define __QUARKDB_RAFT_TALKER_H__

#include "../Common.hh"
#include "../Tunnel.hh"
#include "RaftCommon.hh"
#include <mutex>

namespace quarkdb {

class RaftTalker {
public:
  RaftTalker(const RaftServer &server, const RaftClusterID &clusterID);
  RaftTalker(const RaftServer &server);
  std::future<redisReplyPtr> appendEntries(RaftTerm term, RaftServer leader, LogIndex prevIndex,
                                           RaftTerm prevTerm, LogIndex commit,
                                           const std::vector<RedisRequest> &reqs,
                                           const std::vector<RaftTerm> &entryTerms);

  std::future<redisReplyPtr> requestVote(const RaftVoteRequest &req);
  std::future<redisReplyPtr> fetch(LogIndex index);
private:
  const RaftClusterID clusterID;
  Tunnel tunnel;
};

}

#endif
