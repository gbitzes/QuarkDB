// ----------------------------------------------------------------------
// File: RaftDispatcher.cc
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

#include "RaftDispatcher.hh"
#include "RaftUtils.hh"
#include "RaftJournal.hh"
#include "RaftWriteTracker.hh"
#include "RaftState.hh"
#include "RaftReplicator.hh"
#include "../StateMachine.hh"
#include "../Formatter.hh"

#include <random>
#include <sys/stat.h>

using namespace quarkdb;

RaftDispatcher::RaftDispatcher(RaftJournal &jour, StateMachine &sm, RaftState &st, RaftClock &rc, RaftWriteTracker &wt, RaftReplicator &rep)
: journal(jour), stateMachine(sm), state(st), raftClock(rc), redisDispatcher(sm), writeTracker(wt), replicator(rep) {
}

LinkStatus RaftDispatcher::dispatch(Connection *conn, RedisRequest &req) {
  switch(req.getCommand()) {
    case RedisCommand::RAFT_INFO: {
      // safe, read-only request, does not need authorization
      if(req.size() == 2 && caseInsensitiveEquals(req[1], "leader")) {
        return conn->string(state.getSnapshot()->leader.toString());
      }

      return conn->statusVector(this->info().toVector());
    }
    case RedisCommand::RAFT_LEADER_INFO: {
      // safe, read-only request, does not need authorization
      RaftStateSnapshotPtr snapshot = state.getSnapshot();
      if(snapshot->status != RaftStatus::LEADER) {
        if(snapshot->leader.empty()) {
          return conn->err("unavailable");
        }
        return conn->moved(0, snapshot->leader);
      }
      return conn->statusVector(this->info().toVector());
    }
    case RedisCommand::RAFT_FETCH_LAST: {
      // safe, read-only request, does not need authorization
      if(req.size() != 2 && req.size() != 3) return conn->errArgs(req[0]);

      int64_t nentries;
      if(!my_strtoll(req[1], nentries) || nentries <= 0) return conn->err(SSTR("could not parse " << req[1]));

      bool raw = false;
      if(req.size() == 3) {
        if(req[2] == "raw") {
          raw = true;
        }
        else {
          return conn->err(SSTR("could not parse " << req[2]));
        }
      }

      std::vector<RaftEntry> entries;
      journal.fetch_last(nentries, entries);

      return conn->raw(Formatter::raftEntries(entries, raw));
    }
    case RedisCommand::RAFT_FETCH: {
      // safe, read-only request, does not need authorization
      if(req.size() != 2 && req.size() != 3) return conn->errArgs(req[0]);

      LogIndex index;
      if(!my_strtoll(req[1], index)) return conn->err(SSTR("could not parse " << req[1]));

      bool raw = false;
      if(req.size() == 3) {
        if(req[2] == "raw") {
          raw = true;
        }
        else {
          return conn->err(SSTR("could not parse " << req[2]));
        }
      }

      RaftEntry entry;
      std::vector<std::string> ret;

      if(this->fetch(index, entry)) {
        return conn->raw(Formatter::raftEntry(entry, raw));
      }

      return conn->null();
    }
    case RedisCommand::RAFT_HEARTBEAT: {
      if(!conn->raftAuthorization) return conn->err("not authorized to issue raft commands");
      RaftHeartbeatRequest dest;
      if(!RaftParser::heartbeat(std::move(req), dest)) {
        return conn->err("malformed request");
      }

      RaftHeartbeatResponse resp = heartbeat(std::move(dest));
      return conn->vector(resp.toVector());
    }
    case RedisCommand::RAFT_APPEND_ENTRIES: {
      if(!conn->raftAuthorization) return conn->err("not authorized to issue raft commands");
      RaftAppendEntriesRequest dest;
      if(!RaftParser::appendEntries(std::move(req), dest)) {
        return conn->err("malformed request");
      }

      RaftAppendEntriesResponse resp = appendEntries(std::move(dest));
      return conn->vector(resp.toVector());
    }
    case RedisCommand::RAFT_REQUEST_VOTE: {
      if(!conn->raftAuthorization) return conn->err("not authorized to issue raft commands");
      RaftVoteRequest votereq;
      if(!RaftParser::voteRequest(req, votereq)) {
        return conn->err("malformed request");
      }

      RaftVoteResponse resp = requestVote(votereq);
      return conn->vector(resp.toVector());
    }
    case RedisCommand::RAFT_HANDSHAKE: {
      conn->raftAuthorization = false;
      if(req.size() != 4) return conn->errArgs(req[0]);

      if(req[2] != journal.getClusterID()) {
        qdb_misconfig("received handshake with wrong cluster id: " << req[2] << " (mine is " << journal.getClusterID() << ")");
        return conn->err("wrong cluster id");
      }

      if(req[3] != raftClock.getTimeouts().toString()) {
        qdb_misconfig("received handshake with different raft timeouts (" << req[3] << ") than mine (" << raftClock.getTimeouts().toString() << ")");
        return conn->err("incompatible raft timeouts");
      }

      conn->raftAuthorization = true;
      return conn->ok();
    }
    case RedisCommand::RAFT_CHECKPOINT: {
      if(req.size() != 2) return conn->errArgs(req[0]);

      std::string err;
      if(!checkpoint(req[1], err)) {
        return conn->err(err);
      }

      return conn->ok();
    }
    case RedisCommand::RAFT_ATTEMPT_COUP: {
      RaftStateSnapshotPtr snapshot = state.getSnapshot();

      if(snapshot->leader.empty()) {
        return conn->err("I have no leader, cannot start a coup");
      }

      if(snapshot->leader == state.getMyself()) {
        return conn->err("I am the leader! I can't revolt against myself, you know.");
      }

      if(!contains(journal.getMembership().nodes, state.getMyself())) {
        return conn->err("I am not a full cluster member, pointless to start a coup. First promote me from observer status.");
      }

      qdb_event("Received request to attempt a coup d'etat against the current leader.");
      raftClock.triggerTimeout();
      return conn->status("vive la revolution");
    }
    case RedisCommand::RAFT_ADD_OBSERVER:
    case RedisCommand::RAFT_REMOVE_MEMBER:
    case RedisCommand::RAFT_PROMOTE_OBSERVER: {
      if(req.size() != 2) return conn->errArgs(req[0]);

      RaftServer srv;
      if(!parseServer(req[1], srv)) {
        return conn->err(SSTR("cannot parse server: " << req[1]));
      }

      RaftStateSnapshotPtr snapshot = state.getSnapshot();
      if(snapshot->status != RaftStatus::LEADER) return conn->err("not a leader");
      if(srv == state.getMyself()) return conn->err("cannot perform membership changes on current leader");

      std::string err;
      bool rc;

      if(req.getCommand() == RedisCommand::RAFT_ADD_OBSERVER) {
        rc = journal.addObserver(snapshot->term, srv, err);
      }
      else if(req.getCommand() == RedisCommand::RAFT_REMOVE_MEMBER) {
        // Build a replication status object with how the full members would
        // look like after the update
        ReplicationStatus replicationStatus = replicator.getStatus();
        replicationStatus.removeReplicas(journal.getMembership().observers);

        ReplicaStatus leaderStatus = { state.getMyself(), true, journal.getLogSize() };
        replicationStatus.addReplica(leaderStatus);
        if(replicationStatus.contains(srv)) {
          replicationStatus.removeReplica(srv);
        }

        if(!replicationStatus.quorumUpToDate(leaderStatus.nextIndex)) {
          return conn->err("membership update blocked, new cluster would not have an up-to-date quorum");
        }
        rc = journal.removeMember(snapshot->term, srv, err);
      }
      else if(req.getCommand() == RedisCommand::RAFT_PROMOTE_OBSERVER) {
        ReplicationStatus replicationStatus = replicator.getStatus();
        if(!replicationStatus.getReplicaStatus(srv).upToDate(journal.getLogSize())) {
          return conn->err("membership update blocked, observer is not up-to-date");
        }

        rc = journal.promoteObserver(snapshot->term, srv, err);
      }
      else {
        qdb_throw("should never happen");
      }

      if(!rc) return conn->err(err);

      // All clear, propagate the update
      replicator.reconfigure();
      return conn->ok();
    }
    case RedisCommand::ACTIVATE_STALE_READS: {
      conn->raftStaleReads = true;
      return conn->ok();
    }
    default: {
      return this->service(conn, req);
    }
  }
}

LinkStatus RaftDispatcher::service(Connection *conn, RedisRequest &req) {
  // A control command should never reach here.
  qdb_assert(req.getCommandType() != CommandType::CONTROL);

  // if not leader, redirect... except if this is a read,
  // and stale reads are active!
  RaftStateSnapshotPtr snapshot = state.getSnapshot();
  if(snapshot->status != RaftStatus::LEADER) {
    if(snapshot->leader.empty()) {
      return conn->err("unavailable");
    }

    if(conn->raftStaleReads && req.getCommandType() == CommandType::READ) {
      // Forward directly to the state machine.
      return redisDispatcher.dispatch(conn, req);
    }

    // Redirect.
    return conn->moved(0, snapshot->leader);
  }

  // read request: What happens if I was just elected as leader, but my state
  // machine is behind leadershipMarker?
  // It means I have committed entries on the journal, which haven't been applied
  // to the state machine. If I were to service a read, I'd be giving out potentially
  // stale values!
  //
  // Ensure the state machine is all caught-up before servicing reads, in order
  // to prevent a linearizability violation.
  if(req.getCommandType() == CommandType::READ) {
    if(stateMachine.getLastApplied() < snapshot->leadershipMarker) {

      // Stall client request until state machine is caught-up, or we lose leadership
      while(!stateMachine.waitUntilTargetLastApplied(snapshot->leadershipMarker, std::chrono::milliseconds(500))) {
        if(snapshot->term != state.getCurrentTerm()) {
          // Ouch, we're no longer a leader.. start from scratch
          return this->service(conn, req);
        }
      }

      // If we've made it this far, the state machine should be all caught-up
      // by now. Proceed to service this request.
      qdb_assert(snapshot->leadershipMarker <= stateMachine.getLastApplied());
    }

    return conn->addPendingRequest(&redisDispatcher, std::move(req));
  }

  // At this point, the received command *must* be a write - verify!
  if(req.getCommandType() != CommandType::WRITE) {
    qdb_critical("RaftDispatcher: unable to dispatch non-write command: " << req[0]);
    return conn->err("internal dispatching error");
  }

  // send request to the write tracker
  std::lock_guard<std::mutex> lock(raftCommand);

  LogIndex index = journal.getLogSize();

  if(!writeTracker.append(index, RaftEntry(snapshot->term, std::move(req)), conn->getQueue(), redisDispatcher)) {
    qdb_critical("appending write for index = " << index <<
    " and term " << snapshot->term << " failed when servicing client request");
    return conn->err("unknown error");
  }

  return 1;
}

RaftHeartbeatResponse RaftDispatcher::heartbeat(const RaftHeartbeatRequest &req) {
  RaftStateSnapshotPtr snapshot;
  return heartbeat(req, snapshot);
}

RaftHeartbeatResponse RaftDispatcher::heartbeat(const RaftHeartbeatRequest &req, RaftStateSnapshotPtr &snapshot) {

  //----------------------------------------------------------------------------
  // This RPC is a custom extension to raft - coupling appendEntries to
  // heartbeats creates certain issues: We can't aggressively pipeline the
  // replicated entries, for example, out of caution of losing the lease,
  // or the follower timing out, since pipelining will affect latencies of
  // acknowledgement reception.
  //
  // Having a separate RPC which is sent strictly every heartbeat interval in
  // addition to appendEntries should alleviate this, and make the cluster
  // far more robust against spurious timeouts in the presence of pipelined,
  // gigantic in size appendEntries messages.
  //
  // We don't lock raftCommand here - this is intentional! We only access
  // thread-safe objects, thus preventing the possibility of an appendEntries
  // storm blocking the heartbeats.
  //----------------------------------------------------------------------------

  if(req.leader == state.getMyself()) {
    qdb_throw("received heartbeat from myself");
  }

  state.observed(req.term, req.leader);
  snapshot = state.getSnapshot();

  if(state.inShutdown()) {
    return {snapshot->term, false, "in shutdown"};
  }

  if(req.term < snapshot->term) {
    return {snapshot->term, false, "My raft term is newer"};
  }

  qdb_assert(req.term == snapshot->term);

  if(req.leader != snapshot->leader) {
    qdb_throw("Received append entries from " << req.leader.toString() << ", while I believe leader for term " << snapshot->term << " is " << snapshot->leader.toString());
  }

  raftClock.heartbeat();
  return {snapshot->term, true, ""};
}

RaftAppendEntriesResponse RaftDispatcher::appendEntries(RaftAppendEntriesRequest &&req) {
  std::lock_guard<std::mutex> lock(raftCommand);

  //----------------------------------------------------------------------------
  // An appendEntries RPC also serves as a heartbeat. We need to preserve the
  // state snapshot taken inside heartbeat.
  //----------------------------------------------------------------------------

  RaftStateSnapshotPtr snapshot;
  RaftHeartbeatResponse heartbeatResponse = heartbeat({req.term, req.leader}, snapshot);

  if(!heartbeatResponse.nodeRecognizedAsLeader) {
    return {heartbeatResponse.term, journal.getLogSize(), false, heartbeatResponse.err};
  }

  //----------------------------------------------------------------------------
  // The contacting node is recognized as leader, proceed with the
  // requested journal modifications, if any.
  //----------------------------------------------------------------------------

  writeTracker.flushQueues(Formatter::moved(0, snapshot->leader));

  if(!journal.matchEntries(req.prevIndex, req.prevTerm)) {
    return {snapshot->term, journal.getLogSize(), false, "Log entry mismatch"};
  }

  //----------------------------------------------------------------------------
  // Four cases.
  // 1. All entries are new; we're grand. By far the most common case.
  // 2. The leader is sligthly confused and is sending entries that I have
  //    already. Perform a quick check to ensure they're identical to mine and
  //    continue on like nothing happened.
  // 3. Some of the entries are different than mine. This can be caused by mild
  //    log inconsistencies when switching leaders. This is normal and expected
  //    to happen rarely, so let's remove the inconsistent entries.
  // 4. Some of the entries are different, AND they've already been committed
  //    or applied. This is a major safety violation and should never happen.
  //----------------------------------------------------------------------------

  LogIndex firstInconsistency = journal.compareEntries(req.prevIndex+1, req.entries);
  LogIndex appendFrom = firstInconsistency - (req.prevIndex+1);

  // check if ALL entries are duplicates. If so, I don't need to do anything.
  if(appendFrom < LogIndex(req.entries.size()) ) {
    if(firstInconsistency <= journal.getCommitIndex()) {
      qdb_throw("detected inconsistent entries for index " << firstInconsistency << ". "
      << " Leader attempted to overwrite a committed entry with one with different contents.");
    }

    journal.removeEntries(firstInconsistency);

    for(size_t i = appendFrom; i < req.entries.size(); i++) {
      if(!journal.append(req.prevIndex+1+i, req.entries[i])) {
        qdb_warn("something odd happened when adding entries to the journal.. probably a race condition, but should be harmless");
        return {snapshot->term, journal.getLogSize(), false, "Unknown error"};
      }
    }
  }

  journal.setCommitIndex(std::min(journal.getLogSize()-1, req.commitIndex));
  warnIfLagging(req.commitIndex);
  return {snapshot->term, journal.getLogSize(), true, ""};
}

void RaftDispatcher::warnIfLagging(LogIndex leaderCommitIndex) {
  const LogIndex threshold = 10000;
  LogIndex entriesBehind = leaderCommitIndex - journal.getCommitIndex();
  if(entriesBehind > threshold &&
     std::chrono::steady_clock::now() - lastLaggingWarning > std::chrono::seconds(10)) {

    qdb_warn("My commit index is " << entriesBehind << " entries behind that of the leader.");
    lastLaggingWarning = std::chrono::steady_clock::now();
  }
  else if(entriesBehind <= threshold && lastLaggingWarning != std::chrono::steady_clock::time_point()) {
    qdb_info("No longer lagging significantly behind the leader. (" << entriesBehind << " entries)");
    lastLaggingWarning = {};
  }
}

RaftVoteResponse RaftDispatcher::requestVote(RaftVoteRequest &req) {
  std::lock_guard<std::mutex> lock(raftCommand);
  if(req.candidate == state.getMyself()) {
    qdb_throw("received request vote from myself: " << state.getMyself().toString());
  }

  //----------------------------------------------------------------------------
  // Defend against disruptive servers.
  // A node that has been removed from the cluster will often not know that,
  // and will repeatedly start elections trying to depose the current leader,
  // effectively making the cluster unavailable.
  //
  // If this node is not part of the cluster (that I know of) and I am already
  // in contact with the leader, completely ignore its vote request, and don't
  // take its term into consideration.
  //
  // If I don't have a leader, the situation is different though. Maybe this
  // node was added later, and I just don't know about it yet. Process the
  // request normally, since there's no leader to depose of, anyway.
  //----------------------------------------------------------------------------

  if(!contains(state.getNodes(), req.candidate)) {
    RaftStateSnapshotPtr snapshot = state.getSnapshot();
    if(!snapshot->leader.empty()) {
      qdb_misconfig("Non-voting " << req.candidate.toString() << " attempted to disrupt the cluster by starting an election for term " << req.term << ". Ignoring its request - shut down that node!");
      return {snapshot->term, RaftVote::VETO};
    }
    qdb_warn("Non-voting " << req.candidate.toString() << " is requesting a vote, even though it is not a voting member of the cluster as far I know. Will still process its request, since I have no leader.");
  }

  state.observed(req.term, {});
  RaftStateSnapshotPtr snapshot = state.getSnapshot();

  //----------------------------------------------------------------------------
  // If the contacting node were to be elected, would they potentially overwrite
  // any of my committed entries?
  //
  // Raft should prevent this, but let's be extra paranoid and send a 'veto'
  // vote if that's the case. Even a single 'veto' response will prevent a node
  // from ascending, even if they have a quorum of positive votes.
  //
  // If this safety mechanism doesn't work for some reason (the network loses
  // the message, or whatever), this node will simply crash later on
  // with an exception instead of overwriting committed entries, in case the
  // candidate does ascend.
  //
  // Under normal circumstances, a 'veto' vote should never affect the outcome
  // of an election, and it ought to be identical to a 'refused' vote.
  //----------------------------------------------------------------------------

  if(req.lastIndex <= journal.getCommitIndex()) {
    if(req.lastIndex < journal.getLogStart()) {
      qdb_event("Vetoing vote request from " << req.candidate.toString() << " because its lastIndex (" << req.lastIndex << ") is before my log start (" << journal.getLogStart() << ") - way too far behind me.");
      return {snapshot->term, RaftVote::VETO};
    }

    RaftTerm myLastIndexTerm;
    if(!journal.fetch(req.lastIndex, myLastIndexTerm).ok()) {
      qdb_critical("Error when reading journal entry " << req.lastIndex << " when trying to determine if accepting a vote request could potentially overwrite my committed entries.");
      // It could be that I just have a corrupted journal - don't prevent the
      // node from ascending in this case... If I crash afterwards during
      // replication, so be it.
      return {snapshot->term, RaftVote::REFUSED};
    }

    // If the node were to ascend, it'll try and remove my req.lastIndex entry
    // as inconsistent, which I consider committed already... Veto!
    if(req.lastTerm != myLastIndexTerm) {
      qdb_event("Vetoing vote request from " << req.candidate.toString() << " because its ascension would overwrite my committed entry with index " << req.lastIndex);
      return {snapshot->term, RaftVote::VETO};
    }

    if(req.lastIndex+1 <= journal.getCommitIndex()) {
      // If the node were to ascend, it would add a leadership marker, and try
      // to remove my committed req.lastIndex+1 entry as conflicting. Veto!
      qdb_event("Vetoing vote request from " << req.candidate.toString() << " because its ascension would overwrite my committed entry with index " << req.lastIndex+1 << " through the addition of a leadership marker.");
      return {snapshot->term, RaftVote::VETO};
    }
  }

  if(snapshot->term != req.term) {
    qdb_event("Rejecting vote request from " << req.candidate.toString() << " because of a term mismatch: " << snapshot->term << " vs " << req.term);
    return {snapshot->term, RaftVote::REFUSED};
  }

  if(!snapshot->votedFor.empty() && snapshot->votedFor != req.candidate) {
    qdb_event("Rejecting vote request from " << req.candidate.toString() << " since I've voted already in this term (" << snapshot->term << ") for " << snapshot->votedFor.toString());
    return {snapshot->term, RaftVote::REFUSED};
  }

  LogIndex myLastIndex = journal.getLogSize()-1;
  RaftTerm myLastTerm;
  if(!journal.fetch(myLastIndex, myLastTerm).ok()) {
    qdb_critical("Error when reading journal entry " << myLastIndex << " when processing request vote.");
    return {snapshot->term, RaftVote::REFUSED};
  }

  if(req.lastTerm < myLastTerm) {
    qdb_event("Rejecting vote request from " << req.candidate.toString() << " since my log is more up-to-date, based on last term: " << myLastIndex << "," << myLastTerm << " vs " << req.lastIndex << "," << req.lastTerm);
    return {snapshot->term, RaftVote::REFUSED};
  }

  if(req.lastTerm == myLastTerm && req.lastIndex < myLastIndex) {
    qdb_event("Rejecting vote request from " << req.candidate.toString() << " since my log is more up-to-date, based on last index: " << myLastIndex << "," << myLastTerm << " vs " << req.lastIndex << "," << req.lastTerm);
    return {snapshot->term, RaftVote::REFUSED};
  }

  // grant vote
  if(!state.grantVote(req.term, req.candidate)) {
    qdb_warn("RaftState rejected the vote request from " << req.candidate.toString() << " and term " << req.term << " - probably benign race condition?");
    return {snapshot->term, RaftVote::REFUSED};
  }

  raftClock.heartbeat();
  return {snapshot->term, RaftVote::GRANTED};
}

RaftInfo RaftDispatcher::info() {
  std::lock_guard<std::mutex> lock(raftCommand);
  RaftStateSnapshotPtr snapshot = state.getSnapshot();
  RaftMembership membership = journal.getMembership();
  ReplicationStatus replicationStatus = replicator.getStatus();

  return {journal.getClusterID(), state.getMyself(), snapshot->leader, membership.epoch, membership.nodes, membership.observers, snapshot->term, journal.getLogStart(),
          journal.getLogSize(), snapshot->status, journal.getCommitIndex(), stateMachine.getLastApplied(), writeTracker.size(),
          std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - snapshot->timeCreated).count(),
          replicationStatus
        };
}

bool RaftDispatcher::fetch(LogIndex index, RaftEntry &entry) {
  rocksdb::Status st = journal.fetch(index, entry);
  return st.ok();
}

bool RaftDispatcher::checkpoint(const std::string &path, std::string &err) {
  if(mkdir(path.c_str(), 0775) != 0) {
    err = SSTR("Error when creating directory '" << path << "', errno: " << errno);
    return false;
  }

  rocksdb::Status st = stateMachine.checkpoint(SSTR(path << "/state-machine"));
  if(!st.ok()) {
    err = st.ToString();
    return false;
  }

  st = journal.checkpoint(SSTR(path << "/raft-journal"));
  if(!st.ok()) {
    err = st.ToString();
    return false;
  }

  return true;
}
