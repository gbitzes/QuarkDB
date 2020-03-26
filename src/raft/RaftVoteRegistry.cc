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
#include "raft/RaftState.hh"
#include "raft/RaftLease.hh"

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
// Count a specific type of vote
//------------------------------------------------------------------------------
size_t RaftVoteRegistry::count(RaftVote vote) const {
  size_t num = 0;

  for(auto it = mContents.begin(); it != mContents.end(); it++) {
    const SingleVote& sv = it->second;

    if(sv.netError || sv.parseError) {
      continue;
    }

    if(sv.resp.vote == vote) {
      num++;
    }
  }

  return num;
}

//------------------------------------------------------------------------------
// Count network errors
//------------------------------------------------------------------------------
size_t RaftVoteRegistry::countNetworkError() const {
  size_t num = 0;

  for(auto it = mContents.begin(); it != mContents.end(); it++) {
    const SingleVote& sv = it->second;

    if(sv.netError) {
      num++;
    }
  }

  return num;
}

//------------------------------------------------------------------------------
// Count parse errors
//------------------------------------------------------------------------------
size_t RaftVoteRegistry::countParseError() const {
  size_t num = 0;

  for(auto it = mContents.begin(); it != mContents.end(); it++) {
    const SingleVote& sv = it->second;

    if(sv.parseError) {
      num++;
    }
  }

  return num;
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

//------------------------------------------------------------------------------
// Describe outcome
//------------------------------------------------------------------------------
std::string RaftVoteRegistry::describeOutcome() const {
  std::ostringstream ss;

  if(mPreVote) {
    ss << "Pre-vote round";
  }
  else {
    ss << "Election round";
  }

  const ElectionOutcome outcome = determineOutcome();
  const size_t granted = count(RaftVote::GRANTED);
  const size_t refused = count(RaftVote::REFUSED);
  const size_t veto = count(RaftVote::VETO);

  if(outcome == ElectionOutcome::kElected) {
    ss << " successful";
  }
  else {
    ss << " unsuccessful";
  }

  ss << " for term " << mTerm << ". Contacted " << mContents.size() << " nodes,";
  ss << " received " << granted+refused+veto << " replies with a tally of ";
  ss << granted << " positive votes, " << refused << " refused votes, and ";
  ss << veto << " vetoes.";

  if(granted >= calculateQuorumSize(mContents.size()+1) && veto > 0) {
    qdb_critical("Received a quorum of positive votes (" << granted << ") plus vetoes: " << veto);
  }

  return ss.str();
}

//------------------------------------------------------------------------------
// Observe terms and leases
//------------------------------------------------------------------------------
void RaftVoteRegistry::observeTermsAndLeases(RaftState &state, RaftLease &lease,
    std::chrono::steady_clock::time_point broadcastTimepoint) {

  qdb_assert(!mPreVote);

  for(auto it = mContents.begin(); it != mContents.end(); it++) {
    const SingleVote& sv = it->second;

    if(sv.netError || sv.parseError) {
      continue;
    }

    state.observed(sv.resp.term, {});
    if(sv.resp.vote == RaftVote::GRANTED) {
      lease.getHandler(it->first).heartbeat(broadcastTimepoint);
    }
  }
}

}