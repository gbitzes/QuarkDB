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

#include "raft/RaftDispatcher.hh"
#include "raft/RaftUtils.hh"
#include "raft/RaftJournal.hh"
#include "raft/RaftWriteTracker.hh"
#include "raft/RaftState.hh"
#include "raft/RaftReplicator.hh"
#include "redis/LeaseFilter.hh"
#include "StateMachine.hh"
#include "Formatter.hh"
#include "utils/ParseUtils.hh"
#include "utils/CommandParsing.hh"
#include "Version.hh"

#include <random>
#include <sys/stat.h>

using namespace quarkdb;

RaftDispatcher::RaftDispatcher(RaftJournal &jour, StateMachine &sm, RaftState &st, RaftHeartbeatTracker &rht, RaftWriteTracker &wt, RaftReplicator &rep, Publisher &pub)
: journal(jour), stateMachine(sm), state(st), heartbeatTracker(rht), redisDispatcher(sm, pub), writeTracker(wt), replicator(rep), publisher(pub) {
}

void RaftDispatcher::notifyDisconnect(Connection *conn) {
  publisher.notifyDisconnect(conn);
}

LinkStatus RaftDispatcher::dispatchInfo(Connection *conn, RedisRequest &req) {
  if(req.size() == 2 && caseInsensitiveEquals(req[1], "leader")) {
    return conn->string(state.getSnapshot()->leader.toString());
  }

  return conn->statusVector(this->info().toVector());
}

LinkStatus RaftDispatcher::dispatch(Connection *conn, Transaction &transaction) {
  return this->service(conn, transaction);
}

LinkStatus RaftDispatcher::dispatchPubsub(Connection *conn, RedisRequest &req) {
  // Only leaders should service pubsub requests.
  RaftStateSnapshotPtr snapshot = state.getSnapshot();
  if(snapshot->status != RaftStatus::LEADER) {
    if(snapshot->leader.empty()) {
      return conn->raw(Formatter::err("unavailable"));
    }

    // Redirect.
    return conn->raw(Formatter::moved(0, snapshot->leader));
  }

  // We're good, submit to publisher.
  return publisher.dispatch(conn, req);
}

LinkStatus RaftDispatcher::dispatch(Connection *conn, RedisRequest &req) {
  if(req.getCommandType() == CommandType::PUBSUB) {
    return dispatchPubsub(conn, req);
  }

  switch(req.getCommand()) {
    case RedisCommand::RAFT_INFO: {
      // safe, read-only request, does not need authorization
      return dispatchInfo(conn, req);
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
      return dispatchInfo(conn, req);
    }
    case RedisCommand::RAFT_FETCH_LAST: {
      // safe, read-only request, does not need authorization
      if(req.size() != 2 && req.size() != 3) return conn->errArgs(req[0]);

      int64_t nentries;
      if(!ParseUtils::parseInt64(req[1], nentries) || nentries <= 0) return conn->err(SSTR("could not parse " << req[1]));

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
    if(!ParseUtils::parseInt64(req[1], index)) return conn->err(SSTR("could not parse " << req[1]));

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
      Connection::FlushGuard guard(conn);

      if(!conn->raftAuthorization) return conn->err("not authorized to issue raft commands");
      RaftAppendEntriesRequest dest;
      if(!RaftParser::appendEntries(std::move(req), dest)) {
        return conn->err("malformed request");
      }

      RaftAppendEntriesResponse resp = appendEntries(std::move(dest));
      return conn->vector(resp.toVector());
    }
    case RedisCommand::RAFT_SET_FSYNC_POLICY: {
      if(req.size() != 2u) return conn->errArgs(req[0]);

      FsyncPolicy policy;
      if(!parseFsyncPolicy(req[1], policy)) {
        return conn->err(SSTR("could not parse '" << req[1] << "', available choices: always,async,sync-important-updates"));
      }

      journal.setFsyncPolicy(policy);
      return conn->ok();
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

      if(req[3] != heartbeatTracker.getTimeouts().toString()) {
        qdb_misconfig("received handshake with different raft timeouts (" << req[3] << ") than mine (" << heartbeatTracker.getTimeouts().toString() << ")");
        return conn->err("incompatible raft timeouts");
      }

      conn->raftAuthorization = true;
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
      heartbeatTracker.triggerTimeout();
      return conn->status("vive la revolution");
    }
    case RedisCommand::RAFT_ADD_OBSERVER:
    case RedisCommand::RAFT_REMOVE_MEMBER:
    case RedisCommand::RAFT_PROMOTE_OBSERVER: {
      std::scoped_lock lock(raftCommand);
      // We need to lock the journal for writes during a membership update.
      // Otherwise, a different client might race to acquire the same position
      // in the journal to place a different entry, and cause a crash.

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
    case RedisCommand::RAFT_JOURNAL_SCAN: {
      if(req.size() <= 1) {
        return conn->errArgs(req[0]);
      }

      ScanCommandArguments args = parseScanCommand(req.begin()+1, req.end(), true);
      if(!args.error.empty()) {
        return conn->err(args.error);
      }

      LogIndex cursor;
      if(!ParseUtils::parseInt64(args.cursor, cursor)) {
        return conn->err(SSTR("invalid cursor: " << args.cursor));
      }

      std::vector<RaftEntryWithIndex> entries;
      LogIndex nextCursor;

      rocksdb::Status st = journal.scanContents(cursor, args.count, args.match, entries, nextCursor);
      if(!st.ok()) {
        return conn->raw(Formatter::fromStatus(st));
      }

      return conn->raw(Formatter::journalScan(nextCursor, entries));
    }
    default: {
      // Must be either a read, or write at this point.
      qdb_assert(req.getCommandType() == CommandType::WRITE || req.getCommandType() == CommandType::READ);
      Transaction tx(std::move(req));
      return this->service(conn, tx);
    }
  }
}

LinkStatus RaftDispatcher::service(Connection *conn, Transaction &tx) {

  // if not leader, redirect... except if this is a read,
  // and stale reads are active!
  RaftStateSnapshotPtr snapshot = state.getSnapshot();
  if(snapshot->status != RaftStatus::LEADER) {
    if(snapshot->leader.empty()) {
      return conn->raw(Formatter::multiply(Formatter::err("unavailable"), tx.expectedResponses()));
    }

    if(conn->raftStaleReads && !tx.containsWrites()) {
      // Forward directly to the state machine.
      return redisDispatcher.dispatch(conn, tx);
    }

    // Redirect.
    return conn->raw(Formatter::multiply(Formatter::moved(0, snapshot->leader), tx.expectedResponses()));
  }

  // What happens if I was just elected as leader, but my state machine is
  // behind leadershipMarker?
  //
  // It means I have committed entries on the journal, which haven't been applied
  // to the state machine. If I were to service a read, I'd be giving out potentially
  // stale values!
  //
  // Ensure the state machine is all caught-up before servicing reads, in order
  // to prevent a linearizability violation.
  //
  // But we do the same thing for writes:
  // - Ensures a leader is stable before actually inserting writes into the
  //   journal.
  // - Ensures no race conditions exist between committing the leadership marker
  //   (which causes a hard-synchronization of the dynamic clock to the static
  //   one), and the time we service lease requests.
  //
  // This adds some latency to writes right after a leader is elected, as we
  // need some extra roundtrips to commit the leadership marker. But since
  // leaders usually last weeks, who cares.
  if(stateMachine.getLastApplied() < snapshot->leadershipMarker) {
    // Stall client request until state machine is caught-up, or we lose leadership
    while(!stateMachine.waitUntilTargetLastApplied(snapshot->leadershipMarker, std::chrono::milliseconds(500))) {
      if(!state.isSnapshotCurrent(snapshot.get())) {
        // Ouch, we're no longer a leader.. start from scratch
        return this->service(conn, tx);
      }
    }

    // If we've made it this far, the state machine should be all caught-up
    // by now. Proceed to service this request.
    qdb_assert(snapshot->leadershipMarker <= stateMachine.getLastApplied());
  }

  if(!tx.containsWrites()) {
    // Forward request to the state machine, without going through the
    // raft journal.
    return conn->addPendingTransaction(&redisDispatcher, std::move(tx));
  }

  // At this point, the received command *must* be a write - verify!
  qdb_assert(tx.containsWrites());

  // Do lease filtering
  ClockValue txTimestamp = stateMachine.getDynamicClock();
  LeaseFilter::transform(tx, txTimestamp);

  // send request to the write tracker
  std::scoped_lock lock(raftCommand);

  LogIndex index = journal.getLogSize();

  if(!writeTracker.append(index, snapshot->term, std::move(tx), conn->getQueue(), redisDispatcher)) {
    // We were most likely hit by the following race:
    // - We retrieved the state snapshot.
    // - The raft term was changed in the meantime, we lost leadership.
    // - The journal rejected the entry due to term mismatch.
    // Let's simply retry.
    return this->service(conn, tx);
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

  if(snapshot->status == RaftStatus::SHUTDOWN) {
    return {snapshot->term, false, "in shutdown"};
  }

  if(req.term < snapshot->term) {
    return {snapshot->term, false, "My raft term is newer"};
  }

  qdb_assert(req.term == snapshot->term);

  if(req.leader != snapshot->leader) {
    qdb_throw("Received append entries from " << req.leader.toString() << ", while I believe leader for term " << snapshot->term << " is " << snapshot->leader.toString());
  }

  heartbeatTracker.heartbeat(std::chrono::steady_clock::now());
  return {snapshot->term, true, ""};
}

RaftAppendEntriesResponse RaftDispatcher::appendEntries(RaftAppendEntriesRequest &&req) {
  std::scoped_lock lock(raftCommand);

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
  publisher.purgeListeners(Formatter::moved(0, snapshot->leader));

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

    if(firstInconsistency != journal.getLogSize() && firstInconsistency <= stateMachine.getLastApplied()) {
      qdb_throw("raft invariant violation: Attempted to remove already applied entries as inconsistent. (first inconsistency: " << firstInconsistency << ", last applied: " << stateMachine.getLastApplied());
    }

    journal.removeEntries(firstInconsistency);

    for(size_t i = appendFrom; i < req.entries.size(); i++) {
      if(!journal.append(req.prevIndex+1+i, req.entries[i], false)) {
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

RaftVoteResponse RaftDispatcher::requestVote(const RaftVoteRequest &req, bool preVote) {
  std::string reqDescr = req.describe(preVote);

  std::scoped_lock lock(raftCommand);
  if(req.candidate == state.getMyself()) {
    qdb_throw("received vote request from myself: " << reqDescr);
  }

  if(!contains(state.getNodes(), req.candidate)) {
    qdb_warn("Non-voting " << req.candidate.toString() << " is requesting a vote, even though it is not a voting member of the cluster as far I know.");
  }

  if(!preVote) {
    state.observed(req.term, {});
  }

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
      qdb_event("Vetoing " << reqDescr << " because its lastIndex (" << req.lastIndex << ") is before my log start (" << journal.getLogStart() << ") - way too far behind me.");
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
      qdb_event("Vetoing " << reqDescr << " because its ascension would overwrite my committed entry with index " << req.lastIndex);
      return {snapshot->term, RaftVote::VETO};
    }

    if(req.lastIndex+1 <= journal.getCommitIndex()) {
      // If the node were to ascend, it would add a leadership marker, and try
      // to remove my committed req.lastIndex+1 entry as conflicting. Veto!
      qdb_event("Vetoing " << reqDescr << " because its ascension would overwrite my committed entry with index " << req.lastIndex+1 << " through the addition of a leadership marker.");
      return {snapshot->term, RaftVote::VETO};
    }
  }

  if(snapshot->term > req.term) {
    qdb_event("Rejecting " << reqDescr << " because of term mismatch: " << snapshot->term << " vs " << req.term);
    return {snapshot->term, RaftVote::REFUSED};
  }

  if(!preVote) {
    qdb_assert(snapshot->term == req.term);
  }

  if(snapshot->term == req.term) {
    if(!snapshot->votedFor.empty() && snapshot->votedFor != req.candidate) {
      qdb_event("Rejecting " << reqDescr << " since I've voted already in this term (" << snapshot->term << ") for " << snapshot->votedFor.toString());
      return {snapshot->term, RaftVote::REFUSED};
    }
  }

  LogIndex myLastIndex = journal.getLogSize()-1;
  RaftTerm myLastTerm;
  if(!journal.fetch(myLastIndex, myLastTerm).ok()) {
    qdb_critical("Error when reading journal entry " << myLastIndex << " when processing request vote.");
    return {snapshot->term, RaftVote::REFUSED};
  }

  if(req.lastTerm < myLastTerm) {
    qdb_event("Rejecting " << reqDescr << " since my journal is more up-to-date, based on last term: " << myLastIndex << "," << myLastTerm << " vs " << req.lastIndex << "," << req.lastTerm);
    return {snapshot->term, RaftVote::REFUSED};
  }

  if(req.lastTerm == myLastTerm && req.lastIndex < myLastIndex) {
    qdb_event("Rejecting " << reqDescr << " since my journal is more up-to-date, based on last index: " << myLastIndex << "," << myLastTerm << " vs " << req.lastIndex << "," << req.lastTerm);
    return {snapshot->term, RaftVote::REFUSED};
  }

  // Grant vote - be generous with the heartbeats to increase robustness.
  // A heartbeat that is registered only _after_ grantVote has been called suffers
  // from the following race:
  // - RaftDirector followerLoop is sleeping in state.wait
  // - grantVote triggers RaftDirector to wake up
  // - HeartbeatTracker has timed-out - followerLoop attempts to start an election,
  //   and all this happens before we reach heartbeatTracker.heartbeat in this thread.
  //
  // ... even though we JUST voted for a different node!
  //
  // Therefore, register the heartbeat twice just to be sure.
  if(!preVote) {
    heartbeatTracker.heartbeat(std::chrono::steady_clock::now());
    if(!state.grantVote(req.term, req.candidate)) {
      qdb_warn("RaftState rejected " << reqDescr << " - probably benign race condition?");
      return {snapshot->term, RaftVote::REFUSED};
    }
    heartbeatTracker.heartbeat(std::chrono::steady_clock::now());
  }

  qdb_event("Granted " << reqDescr);
  return {snapshot->term, RaftVote::GRANTED};
}

//------------------------------------------------------------------------------
// Return health information
//------------------------------------------------------------------------------
NodeHealth RaftDispatcher::getHealth() {
  std::vector<HealthIndicator> indicators = stateMachine.getHealthIndicators();

  //----------------------------------------------------------------------------
  // Am I currently part of the quorum?
  //----------------------------------------------------------------------------
  RaftStateSnapshotPtr snapshot = state.getSnapshot();
  if(snapshot->leader.empty()) {
    indicators.emplace_back(HealthStatus::kRed, "PART-OF-QUORUM", "No");
  }
  else {
    indicators.emplace_back(HealthStatus::kGreen, "PART-OF-QUORUM", SSTR("Yes | LEADER " << snapshot->leader.toString()));
  }

  //----------------------------------------------------------------------------
  // Leader? If so, show replication status
  //----------------------------------------------------------------------------
  if(snapshot->status == RaftStatus::LEADER) {
    ReplicationStatus replicationStatus = replicator.getStatus();
    LogIndex logSize = journal.getLogSize();

    if(replicationStatus.shakyQuorum) {
      indicators.emplace_back(HealthStatus::kYellow, "QUORUM-STABILITY", "Shaky");
    }
    else {
      indicators.emplace_back(HealthStatus::kGreen, "QUORUM-STABILITY", "Good");
    }

    for(auto it = replicationStatus.replicas.begin(); it != replicationStatus.replicas.end(); it++) {
      HealthStatus replicaStatus = HealthStatus::kGreen;

      if(!it->online) {
        replicaStatus = HealthStatus::kYellow;
      }
      else if(!it->upToDate(logSize)) {
        replicaStatus = HealthStatus::kYellow;
      }

      indicators.emplace_back(replicaStatus, "REPLICA", it->toString(logSize));
    }
  }

  return NodeHealth(VERSION_FULL_STRING, state.getMyself().toString(), indicators);
}

RaftInfo RaftDispatcher::info() {
  std::scoped_lock lock(raftCommand);
  RaftStateSnapshotPtr snapshot = state.getSnapshot();
  RaftMembership membership = journal.getMembership();
  ReplicationStatus replicationStatus = replicator.getStatus();
  HealthStatus nodeHealthStatus = chooseWorstHealth(getHealth().getIndicators());

  return {journal.getClusterID(), state.getMyself(), snapshot->leader, nodeHealthStatus, journal.getFsyncPolicy(), membership.epoch, membership.nodes, membership.observers, snapshot->term, journal.getLogStart(),
          journal.getLogSize(), snapshot->status, journal.getCommitIndex(), stateMachine.getLastApplied(), writeTracker.size(),
          std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - snapshot->timeCreated).count(),
          replicationStatus, VERSION_FULL_STRING
        };
}

bool RaftDispatcher::fetch(LogIndex index, RaftEntry &entry) {
  rocksdb::Status st = journal.fetch(index, entry);
  return st.ok();
}
