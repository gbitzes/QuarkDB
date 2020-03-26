// ----------------------------------------------------------------------
// File: RaftVoteRegistry.cc
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

#include "raft/RaftVoteRegistry.hh"
#include "raft/RaftUtils.hh"

namespace quarkdb {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RaftVoteRegistry::RaftVoteRegistry(RaftTerm term, bool prevote)
: mTerm(term), mPreVote(prevote) {}

//------------------------------------------------------------------------------
// Register vote
//------------------------------------------------------------------------------
void RaftVoteRegistry::registerVote(const RaftServer &srv, RaftVoteResponse resp) {
  qdb_assert(mContents.find(srv) == mContents.end());

  SingleVote vote;
  vote.netError = false;
  vote.parseError = false;
  vote.resp = resp;

  mContents[srv] = vote;
}

//------------------------------------------------------------------------------
// Register vote
//------------------------------------------------------------------------------
void RaftVoteRegistry::registerParseError(const RaftServer &srv) {
  qdb_assert(mContents.find(srv) == mContents.end());

  SingleVote vote;
  vote.netError = false;
  vote.parseError = true;

  mContents[srv] = vote;
}

//------------------------------------------------------------------------------
// Register vote
//------------------------------------------------------------------------------
void RaftVoteRegistry::registerNetworkError(const RaftServer &srv) {
  qdb_assert(mContents.find(srv) == mContents.end());

  SingleVote vote;
  vote.netError = true;
  vote.parseError = false;

  mContents[srv] = vote;
}

//------------------------------------------------------------------------------
// Determine outcome
//------------------------------------------------------------------------------
ElectionOutcome RaftVoteRegistry::determineOutcome() const {
  size_t positives = 0;

  for(auto it = mContents.begin(); it != mContents.end(); it++) {
    const SingleVote& sv = it->second;

    if(sv.netError) {
      continue;
    }
    else if(sv.parseError) {
      if(mPreVote) {
        // Does not support pre-vote... assume granted
        positives++;
      }
    }
    else if(sv.resp.vote == RaftVote::GRANTED) {
      positives++;
    }
    else if(sv.resp.vote == RaftVote::VETO) {
      return ElectionOutcome::kVetoed;
    }
  }

  // Implicit vote for myself
  positives++;

  if(positives >= calculateQuorumSize(mContents.size()+1)) {
    return ElectionOutcome::kElected;

  }

  return ElectionOutcome::kNotElected;
}

//------------------------------------------------------------------------------
// Register vote
//------------------------------------------------------------------------------
void RaftVoteRegistry::registerVote(const RaftServer &srv, std::future<qclient::redisReplyPtr> &fut, std::chrono::steady_clock::time_point deadline) {
  if(fut.wait_until(deadline) != std::future_status::ready) {
    return registerNetworkError(srv);
  }

  qclient::redisReplyPtr reply = fut.get();
  if(reply == nullptr) {
    return registerNetworkError(srv);
  }

  RaftVoteResponse resp;
  if(!RaftParser::voteResponse(reply, resp)) {
    if(!mPreVote) {
      qdb_critical("Could not parse vote response from " << srv.toString() << ": " << qclient::describeRedisReply(reply));
    }

    return registerParseError(srv);
  }

  return registerVote(srv, resp);
}

}