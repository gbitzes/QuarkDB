// ----------------------------------------------------------------------
// File: raft.cc
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

#include "raft/Raft.hh"
#include "raft/RaftReplicator.hh"
#include "raft/RaftTalker.hh"
#include "Tunnel.hh"
#include "Poller.hh"
#include "test-utils.hh"
#include "RedisParser.hh"
#include <gtest/gtest.h>

using namespace quarkdb;
#define ASSERT_OK(msg) ASSERT_TRUE(msg.ok())

TEST(RaftReplicator, T1) {
  RaftJournal::ObliterateAndReinitializeJournal(test_journals[0], test_clusterIDs[0], testnodes3);
  RocksDB stateMachine(test_statemachines[0]);
  RaftJournal journal(test_journals[0]);
  RaftState state(journal, testnodes[0]);

  RaftReplicator replicator(journal, state);

  // trying to replicate my own log
  ASSERT_THROW(replicator.launch(testnodes[0], state.getSnapshot()), FatalException);

  // not a leader
  ASSERT_THROW(replicator.launch(testnodes[1], state.getSnapshot()), FatalException);

  // snapshot has larger term than state
  RaftStateSnapshot snapshot = state.getSnapshot();
  snapshot.term = 1;
  ASSERT_THROW(replicator.launch(testnodes[1], snapshot), FatalException);
  snapshot.term = 0;

  ASSERT_TRUE(state.becomeCandidate(0));
  ASSERT_TRUE(state.ascend(0));

  ASSERT_TRUE(state.observed(1, {}));

  // stale term - this can naturally happen, so it is not an exception
  ASSERT_FALSE(replicator.launch(testnodes[1], snapshot));

  ASSERT_TRUE(state.becomeCandidate(1));
  ASSERT_TRUE(state.ascend(1));

  // start replication
  RocksDB stateMachine1(test_statemachines[1]);
  RaftJournal::ObliterateAndReinitializeJournal(test_journals[1], test_clusterIDs[0], testnodes3);
  RaftJournal journal1(test_journals[1]);
  Raft raft(journal1, stateMachine1, testnodes[1]);

  // add an inconsistent journal entry, just for fun
  ASSERT_TRUE(journal1.append(1, 0, make_req("supposed", "to", "be", "removed")));

  RaftInfo info = raft.info();
  ASSERT_EQ(info.term, 0);

  Poller poller(test_unixsockets[1], &raft);
  std::this_thread::sleep_for(std::chrono::milliseconds(2));
  ASSERT_TRUE(replicator.launch(testnodes[1], state.getSnapshot()));

  for(size_t i = 0; i < testreqs.size(); i++) {
    ASSERT_TRUE(journal.append(i+1, 1, testreqs[i]));
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  info = raft.info();
  ASSERT_EQ(info.term, 1);
  ASSERT_EQ(info.logSize, (int64_t) testreqs.size()+1);

  for(size_t i = 0; i < testreqs.size(); i++) {
    RaftEntry entry;
    ASSERT_TRUE(raft.fetch(i+1, entry));
    ASSERT_EQ(entry.term, 1) << "i = " << i;
    ASSERT_EQ(entry.request, testreqs[i]);
  }
}

TEST(Raft, T1) {
  RaftJournal::ObliterateAndReinitializeJournal(test_journals[0], test_clusterIDs[0], testnodes3);

  RocksDB stateMachine(test_statemachines[0]);
  RaftJournal journal0(test_journals[0]);
  Raft raft(journal0, stateMachine, testnodes[2]);

  RaftInfo info = raft.info();
  ASSERT_EQ(info.clusterID, test_clusterIDs[0]);
  ASSERT_EQ(info.myself, testnodes[2]);
  ASSERT_EQ(info.term, 0);
  ASSERT_EQ(info.logSize, 1);

  RaftEntry entry;
  ASSERT_TRUE(raft.fetch(0, entry));
  ASSERT_EQ(entry.term, 0);
  ASSERT_EQ(entry.request, make_req("UPDATE_RAFT_NODES", serializeNodes(testnodes3)));

  // heartbeat, update term
  RaftAppendEntriesRequest req;
  req.term = 1;
  req.leader = testnodes[1];
  req.prevIndex = 0;
  req.prevTerm = 0;
  req.commitIndex = 0;

  RaftAppendEntriesResponse resp = raft.appendEntries(std::move(req));
  ASSERT_EQ(resp.term, 1);
  ASSERT_TRUE(resp.outcome);
  ASSERT_EQ(resp.logSize, 1);

  // append entries from myself, should throw
  req.leader = testnodes[2];
  ASSERT_THROW(raft.appendEntries(std::move(req)), FatalException);

  // add two entries
  req.leader = testnodes[1];

  req.entries.push_back(RaftEntry {1, {"set", "qwery", "123"}});
  req.entries.push_back(RaftEntry {1, {"hset", "abc", "123", "234"}});

  resp = raft.appendEntries(std::move(req));
  ASSERT_EQ(resp.term, 1);
  ASSERT_TRUE(resp.outcome);
  ASSERT_EQ(resp.logSize, 3);

  // previous entry mismatch
  req = {};
  req.term = 1;
  req.leader = testnodes[1];
  req.prevIndex = 2;
  req.prevTerm = 0;
  req.commitIndex = 0;

  resp = raft.appendEntries(std::move(req));
  ASSERT_EQ(resp.term, 1);
  ASSERT_FALSE(resp.outcome);

  // remove inconsistent entry
  req = {};
  req.term = 1;
  req.leader = testnodes[1];
  req.prevIndex = 1;
  req.prevTerm = 1;
  req.commitIndex = 0;

  resp = raft.appendEntries(std::move(req));
  ASSERT_EQ(resp.term, 1);
  ASSERT_EQ(resp.logSize, 2);

  // add three entries with different leader
  req = {};
  req.term = 5;
  req.leader = testnodes[0];
  req.prevIndex = 1;
  req.prevTerm = 1;
  req.commitIndex = 1;

  req.entries.push_back(RaftEntry {2, {"sadd", "myset", "a"}});
  req.entries.push_back(RaftEntry {2, {"sadd", "myset", "b"}});
  req.entries.push_back(RaftEntry {3, {"sadd", "myset", "c"}});

  resp = raft.appendEntries(std::move(req));
  ASSERT_EQ(resp.term, 5);
  ASSERT_EQ(resp.logSize, 5);
  ASSERT_TRUE(resp.outcome);

  ASSERT_TRUE(raft.fetch(2, entry));
  ASSERT_EQ(entry.term, 2);
  ASSERT_EQ(entry.request, make_req("sadd", "myset", "a"));
}
