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

#include "utils/IntToBinaryString.hh"
#include "RaftUtils.hh"
#include "RaftTalker.hh"
#include "RaftState.hh"
#include "RaftLease.hh"
#include "RaftContactDetails.hh"
#include "utils/ParseUtils.hh"
#include "utils/StringUtils.hh"

namespace quarkdb {

static ElectionSingleTally parseSingleTally(std::future<qclient::redisReplyPtr> &fut, std::chrono::steady_clock::time_point deadline) {
  ElectionSingleTally singleTally;

  if(fut.wait_until(deadline) != std::future_status::ready) {
    singleTally.timeout = true;
    singleTally.error = false;
    return singleTally;
  }

  redisReplyPtr reply = fut.get();
  if(reply == nullptr || !RaftParser::voteResponse(reply, singleTally.resp)) {
    singleTally.timeout = false;
    singleTally.error = true;
    return singleTally;
  }

  singleTally.timeout = false;
  singleTally.error = false;
  return singleTally;
}

static ElectionTally makeElectionTally(RaftVoteRequest votereq, const std::vector<RaftServer> &nodes, const RaftContactDetails &contactDetails) {
  qdb_info("Starting election round for term " << votereq.term);

  std::vector<std::unique_ptr<RaftTalker>> talkers;
  std::map<RaftServer, std::future<redisReplyPtr>> futures;

  for(const RaftServer &node : nodes) {
    if(node != votereq.candidate) {
      talkers.emplace_back(new RaftTalker(node, contactDetails, "internal-vote-request"));
      futures[node] = talkers.back()->requestVote(votereq);
    }
  }

  std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  std::chrono::steady_clock::time_point deadline = now + contactDetails.getRaftTimeouts().getHeartbeatInterval()*2;

  qdb_info("Vote requests have been sent off, will allow a window of "
    << contactDetails.getRaftTimeouts().getLow().count() << "ms to receive replies.");

  ElectionTally tally;
  for(auto it = futures.begin(); it != futures.end(); it++) {
    tally[it->first] = parseSingleTally(it->second, deadline);
  }

  qdb_info("No longer accepting replies to vote requests, time to make a tally.");
  return tally;
}

ElectionOutcome RaftElection::perform(RaftVoteRequest votereq, RaftState &state, RaftLease &lease, const RaftContactDetails &contactDetails) {
  if(!votereq.candidate.empty()) {
    qdb_throw("candidate member of votereq must be empty, it is filled out by this function");
  }

  votereq.candidate = state.getMyself();
  RaftStateSnapshotPtr snapshot = state.getSnapshot();

  if(votereq.term != snapshot->term) {
    qdb_warn("Aborting election, received stale term: " << votereq.term << " vs " << snapshot->term);
    return ElectionOutcome::kNotElected;
  }

  if(!snapshot->leader.empty()) {
    qdb_warn("Aborting election, we already have a recognized leader already for term " << snapshot->term << " which is " << snapshot->leader.toString());
    return ElectionOutcome::kNotElected;
  }

  if(snapshot->status != RaftStatus::CANDIDATE) {
    qdb_warn("Aborting election, I am not a candidate for " << snapshot->term << ", but in status " << statusToString(snapshot->status));
    return ElectionOutcome::kNotElected;
  }


  std::chrono::steady_clock::time_point broadcastTimepoint = std::chrono::steady_clock::now();
  ElectionTally tally = makeElectionTally(votereq, state.getNodes(), contactDetails);

  size_t granted = 0;
  size_t refused = 0;
  size_t veto = 0;
  size_t replies = 0;

  for(auto it = tally.begin(); it != tally.end(); it++) {
    ElectionSingleTally singleTally = it->second;

    if(singleTally.timeout || singleTally.error) {
      continue;
    }

    if(singleTally.resp.vote == RaftVote::GRANTED) {
      lease.getHandler(it->first).heartbeat(broadcastTimepoint);
      granted++;
    }
    else if(singleTally.resp.vote == RaftVote::REFUSED) {
      refused++;
    }
    else if(singleTally.resp.vote == RaftVote::VETO) {
      veto++;
    }

    state.observed(singleTally.resp.term, {});
  }

  replies = granted + refused + veto;

  std::string description = SSTR("Contacted " << tally.size() << " nodes, received "
    << replies << " replies with a tally of " << granted << " positive votes, " << refused << " refused votes, and " << veto << " vetoes.");

  if(granted+1 >= (state.getNodes().size() / 2)+1 ) {
    if(veto > 0) {
      qdb_critical("Election round unsuccessful for term " << votereq.term << " because of vetoes, even though I received a quorum of positive votes. (!!!) " << description);
      return ElectionOutcome::kVetoed;
    }
    qdb_event("Election round successful for term " << votereq.term << ". " << description);
    if(state.ascend(votereq.term)) {
      return ElectionOutcome::kElected;
    }

    // Some strange race condition occured, term must have progressed.
    return ElectionOutcome::kNotElected;
  }
  else {
    qdb_event("Election round unsuccessful for term " << votereq.term << ", did not receive a quorum of votes. " << description);

    if(veto > 0) {
      return ElectionOutcome::kVetoed;
    }

    return ElectionOutcome::kNotElected;
  }
}

bool RaftParser::appendEntries(RedisRequest &&source, RaftAppendEntriesRequest &dest) {
  //----------------------------------------------------------------------------
  // We assume source[0] is correct, ie "raft_append_entries"
  //----------------------------------------------------------------------------

  // 3 chunks is the minimum for a 0-entries request
  if(source.size() < 3) return false;

  if(!parseServer(source[1], dest.leader)) return false;
  if(source[2].size() != sizeof(int64_t) * 5) return false;
  int64_t nreqs;

  dest.term        = binaryStringToInt(source[2].data() + 0*sizeof(int64_t) );
  dest.prevIndex   = binaryStringToInt(source[2].data() + 1*sizeof(int64_t) );
  dest.prevTerm    = binaryStringToInt(source[2].data() + 2*sizeof(int64_t) );
  dest.commitIndex = binaryStringToInt(source[2].data() + 3*sizeof(int64_t) );
  nreqs            = binaryStringToInt(source[2].data() + 4*sizeof(int64_t) );

  if((int) source.size() != 3 + nreqs) return false;
  dest.entries.resize(nreqs);

  int64_t index = 3;
  for(int64_t i = 0; i < nreqs; i++) {
    RaftEntry::deserialize(dest.entries[i], source[index]);
    index++;
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

  std::string_view tmp(source->element[0]->str, source->element[0]->len);
  if(!ParseUtils::parseInt64(tmp, dest.term)) return false;

  tmp = std::string_view(source->element[1]->str, source->element[1]->len);
  if(!ParseUtils::parseInt64(tmp, dest.logSize)) return false;

  tmp = std::string_view(source->element[2]->str, source->element[2]->len);
  if(tmp == "0") dest.outcome = false;
  else if(tmp == "1") dest.outcome = true;
  else return false;

  dest.err = std::string(source->element[3]->str, source->element[3]->len);
  return true;
}

bool RaftParser::heartbeat(const RedisRequest &source, RaftHeartbeatRequest &dest) {
  //----------------------------------------------------------------------------
  // We assume source[0] is correct, ie "raft_heartbeat"
  //----------------------------------------------------------------------------

  if(source.size() != 3) return false;

  if(!ParseUtils::parseInt64(source[1], dest.term)) return false;
  if(!parseServer(source[2], dest.leader)) return false;

  return true;
}

bool RaftParser::heartbeatResponse(const qclient::redisReplyPtr &source, RaftHeartbeatResponse &dest) {
  if(source == nullptr || source->type != REDIS_REPLY_ARRAY || source->elements != 3) {
    return false;
  }

  for(size_t i = 0; i < source->elements; i++) {
    if(source->element[i]->type != REDIS_REPLY_STRING) {
      return false;
    }
  }

  std::string_view tmp(source->element[0]->str, source->element[0]->len);
  if(!ParseUtils::parseInt64(tmp, dest.term)) return false;

  tmp = std::string_view(source->element[1]->str, source->element[1]->len);
  if(tmp == "0") dest.nodeRecognizedAsLeader = false;
  else if(tmp == "1") dest.nodeRecognizedAsLeader = true;
  else return false;

  dest.err = std::string(source->element[2]->str, source->element[2]->len);
  return true;
}

bool RaftParser::voteRequest(RedisRequest &source, RaftVoteRequest &dest) {
  //----------------------------------------------------------------------------
  // We assume source[0] is correct, ie "raft_request_vote"
  //----------------------------------------------------------------------------

  if(source.size() != 5) return false;

  if(!ParseUtils::parseInt64(source[1], dest.term)) return false;
  if(!parseServer(source[2], dest.candidate)) return false;
  if(!ParseUtils::parseInt64(source[3], dest.lastIndex)) return false;
  if(!ParseUtils::parseInt64(source[4], dest.lastTerm)) return false;
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

  std::string_view tmp(source->element[0]->str, source->element[0]->len);
  if(!ParseUtils::parseInt64(tmp, dest.term)) return false;

  tmp = std::string_view(source->element[1]->str, source->element[1]->len);
  if(tmp == "granted") {
    dest.vote = RaftVote::GRANTED;
  }
  else if(tmp == "refused") {
    dest.vote = RaftVote::REFUSED;
  }
  else if(tmp == "veto") {
    dest.vote = RaftVote::VETO;
  }
  else {
    return false; // parse error
  }

  return true;
}

bool RaftParser::fetchResponse(redisReply *source, RaftEntry &entry) {
  if(source == nullptr || source->type != REDIS_REPLY_ARRAY || source->elements != 2) {
    return false;
  }

  if(source->element[0]->type != REDIS_REPLY_STRING) {
    return false;
  }

  if(source->element[1]->type != REDIS_REPLY_ARRAY) {
    return false;
  }

  redisReply *req = source->element[1];

  for(size_t i = 0; i < req->elements; i++) {
    if(req->element[i]->type != REDIS_REPLY_STRING) {
      return false;
    }
  }

  std::string_view tmp(source->element[0]->str, source->element[0]->len);
  if(!StringUtils::startsWith(tmp, "TERM: ")) return false;
  tmp = std::string_view(tmp.data()+ 6, tmp.size()-6);

  if(!ParseUtils::parseInt64(tmp, entry.term)) return false;

  entry.request.clear();
  for(size_t i = 0; i < req->elements; i++) {
    entry.request.emplace_back(req->element[i]->str, req->element[i]->len);
  }

  return true;
}

bool RaftParser::fetchLastResponse(const qclient::redisReplyPtr &source, std::vector<RaftEntry> &entries) {
  if(source == nullptr || source->type != REDIS_REPLY_ARRAY) {
    return false;
  }

  entries.clear();
  entries.resize(source->elements);

  for(size_t i = 0; i < source->elements; i++) {
    if(!fetchResponse(source->element[i], entries[i])) {
      return false;
    }
  }

  return true;
}

}
