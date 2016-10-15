// ----------------------------------------------------------------------
// File: RaftUtils.cc
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

#include "RaftUtils.hh"
#include "RaftTalker.hh"
#include "../Utils.hh"
using namespace quarkdb;

bool RaftElection::perform(RaftVoteRequest votereq, RaftState &state, const RaftTimeouts timeouts) {
  if(!votereq.candidate.empty()) {
    qdb_throw("candidate member of votereq must be empty, it is filled out by this function");
  }

  votereq.candidate = state.getMyself();
  RaftStateSnapshot snapshot = state.getSnapshot();

  if(votereq.term != snapshot.term) {
    qdb_warn("Aborting election, received stale term: " << votereq.term << " vs " << snapshot.term);
    return false;
  }

  if(!snapshot.leader.empty()) {
    qdb_warn("Aborting election, we already have a recognized leader already for term " << snapshot.term << " which is " << snapshot.leader.toString());
    return false;
  }

  if(snapshot.status != RaftStatus::CANDIDATE) {
    qdb_warn("Aborting election, I am not a candidate for " << snapshot.term << ", but in status " << statusToString(snapshot.status));
    return false;
  }

  qdb_info(state.getMyself().toString() << ": Starting election round for term " << votereq.term);
  std::vector<RaftTalker*> talkers;

  std::vector<std::future<redisReplyPtr>> futures;
  for(const RaftServer &node : state.getNodes()) {
    if(node != state.getMyself()) {
      talkers.push_back(new RaftTalker(node, state.getClusterID()));
      futures.push_back(talkers.back()->requestVote(votereq));
    }
  }

  std::vector<redisReplyPtr> replies;
  std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  std::chrono::steady_clock::time_point deadline = now + timeouts.getHeartbeatInterval()*2;

  qdb_info(state.getMyself().toString() <<  ": Vote requests have been sent off, will allow a window of "
    << timeouts.getHeartbeatInterval().count()*2 << "ms to receive replies.");

  for(size_t i = 0; i < futures.size(); i++) {
    if(futures[i].wait_until(deadline) == std::future_status::ready) {
      redisReplyPtr reply = futures[i].get();
      if(reply != nullptr) replies.push_back(reply);
    }
  }

  qdb_info("No longer accepting replies to vote requests, time to make a tally.");

  size_t tally = 0;
  for(size_t i = 0; i < replies.size(); i++) {
    RaftVoteResponse resp;
    if(!RaftParser::voteResponse(replies[i], resp)) {
      qdb_critical("unable to parse a vote response, ignoring");
    }
    else {
      if(resp.granted) tally++;
      state.observed(resp.term, {});
    }
  }

  for(RaftTalker* talker : talkers) {
    delete talker;
  }

  std::string description = SSTR("Contacted " << futures.size() << " nodes, received "
    << replies.size() << " replies with a tally of " << tally << " positive votes.");

  if(tally+1 >= (state.getNodes().size() / 2)+1 ) {
    qdb_event("Election round successful for term " << votereq.term << ". " << description);
    return state.ascend(votereq.term);
  }
  else {
    qdb_event("Election round unsuccessful for term " << votereq.term << ", did not receive a quorum of votes. " << description);
    return false;
  }
}

bool RaftParser::appendEntries(RedisRequest &&source, RaftAppendEntriesRequest &dest) {
  //----------------------------------------------------------------------------
  // We assume source[0] is correct, ie "raft_append_entries"
  //----------------------------------------------------------------------------

  // 7 chunks is the minimum for a 0-entries request
  if(source.size() < 7) return false;

  if(!my_strtoll(source[1], dest.term)) return false;
  if(!parseServer(source[2], dest.leader)) return false;
  if(!my_strtoll(source[3], dest.prevIndex)) return false;
  if(!my_strtoll(source[4], dest.prevTerm)) return false;
  if(!my_strtoll(source[5], dest.commitIndex)) return false;

  int64_t nreqs;
  if(!my_strtoll(source[6], nreqs)) return false;
  if((int) source.size() < 7 + (nreqs*3)) return false;

  int64_t index = 7;
  for(int64_t i = 0; i < nreqs; i++) {
    int64_t reqsize;
    if(!my_strtoll(source[index], reqsize)) return false;
    if((int) source.size() < index+2+reqsize) return false;

    RaftEntry tmp;
    if(!my_strtoll(source[index+1], tmp.term)) return false;
    for(int64_t j = 0; j < reqsize; j++) {
      tmp.request.emplace_back(std::move(source[index+2+j]));
    }

    dest.entries.emplace_back(std::move(tmp));
    index += 2+reqsize;
  }

  if(index != (int64_t) source.size()) return false;
  return true;
}

bool RaftParser::appendEntriesResponse(const redisReplyPtr &source, RaftAppendEntriesResponse &dest) {
  if(source == nullptr || source->type != REDIS_REPLY_ARRAY || source->elements != 4) {
    return false;
  }

  for(size_t i = 0; i < source->elements; i++) {
    if(source->element[i]->type != REDIS_REPLY_STRING) {
      return false;
    }
  }

  std::string tmp(source->element[0]->str, source->element[0]->len);
  if(!my_strtoll(tmp, dest.term)) return false;

  tmp = std::string(source->element[1]->str, source->element[1]->len);
  if(!my_strtoll(tmp, dest.logSize)) return false;

  tmp = std::string(source->element[2]->str, source->element[2]->len);
  if(tmp == "0") dest.outcome = false;
  else if(tmp == "1") dest.outcome = true;
  else return false;

  dest.err = std::string(source->element[3]->str, source->element[3]->len);
  return true;
}

bool RaftParser::voteRequest(RedisRequest &source, RaftVoteRequest &dest) {
  //----------------------------------------------------------------------------
  // We assume source[0] is correct, ie "raft_request_vote"
  //----------------------------------------------------------------------------

  if(source.size() != 5) return false;

  if(!my_strtoll(source[1], dest.term)) return false;
  if(!parseServer(source[2], dest.candidate)) return false;
  if(!my_strtoll(source[3], dest.lastIndex)) return false;
  if(!my_strtoll(source[4], dest.lastTerm)) return false;
  return true;
}

bool RaftParser::voteResponse(const redisReplyPtr &source, RaftVoteResponse &dest) {
  if(source == nullptr || source->type != REDIS_REPLY_ARRAY || source->elements != 2) {
    return false;
  }

  for(size_t i = 0; i < source->elements; i++) {
    if(source->element[i]->type != REDIS_REPLY_STRING) {
      return false;
    }
  }

  std::string tmp(source->element[0]->str, source->element[0]->len);
  if(!my_strtoll(tmp, dest.term)) return false;

  tmp = std::string(source->element[1]->str, source->element[1]->len);
  if(tmp == "0") dest.granted = false;
  else if(tmp == "1") dest.granted = true;
  else return false;

  return true;
}
