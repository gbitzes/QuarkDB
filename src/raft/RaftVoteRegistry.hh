// ----------------------------------------------------------------------
// File: RaftVoteRegistry.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#ifndef QUARKDB_RAFT_VOTE_REGISTRY_HH
#define QUARKDB_RAFT_VOTE_REGISTRY_HH

#include "raft/RaftCommon.hh"
#include <future>
#include "qclient/QClient.hh"

namespace quarkdb {

//------------------------------------------------------------------------------
// Helper class for counting votes received during an election
//------------------------------------------------------------------------------
class RaftVoteRegistry {
public:
  //----------------------------------------------------------------------------
  // Hold the response for a single server
  //----------------------------------------------------------------------------
  struct SingleVote {
    bool netError;
    bool parseError;
    RaftVoteResponse resp;
  };

  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  RaftVoteRegistry(RaftTerm term, bool prevote);

  //----------------------------------------------------------------------------
  // Register vote
  //----------------------------------------------------------------------------
  void registerVote(const RaftServer &srv, RaftVoteResponse resp);

  //----------------------------------------------------------------------------
  // Register vote
  //----------------------------------------------------------------------------
  void registerParseError(const RaftServer &srv);

  //----------------------------------------------------------------------------
  // Register vote
  //----------------------------------------------------------------------------
  void registerNetworkError(const RaftServer &srv);

  //----------------------------------------------------------------------------
  // Register vote
  //----------------------------------------------------------------------------
  void registerVote(const RaftServer &srv, std::future<qclient::redisReplyPtr> &fut,
    std::chrono::steady_clock::time_point deadline);

  //----------------------------------------------------------------------------
  // Determine outcome
  //----------------------------------------------------------------------------
  ElectionOutcome determineOutcome() const;


private:
  RaftTerm mTerm;
  bool mPreVote;

  std::map<RaftServer, SingleVote> mContents;
};

}

#endif