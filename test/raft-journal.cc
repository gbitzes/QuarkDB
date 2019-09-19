// ----------------------------------------------------------------------
// File: raft-journal.cc
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

#include "raft/RaftJournal.hh"
#include "test-utils.hh"
#include <gtest/gtest.h>

using namespace quarkdb;
#define ASSERT_OK(msg) ASSERT_TRUE(msg.ok())
#define ASSERT_NOTFOUND(msg) ASSERT_TRUE(msg.IsNotFound())

class Raft_Journal : public ::testing::Test {
protected:
  virtual void SetUp() {
    nodes.emplace_back("server1", 7776);
    nodes.emplace_back("server2", 7777);
    nodes.emplace_back("server3", 7778);
    RaftJournal::ObliterateAndReinitializeJournal(dbpath, clusterID, nodes, 0);
  }

  virtual void TearDown() { }

  std::vector<RaftServer> nodes;
  std::vector<RaftServer> observers;
  std::string dbpath = "/tmp/raft-journal";
  RaftClusterID clusterID = "55cd595d-7306-4971-b92c-4b9ba5930d40";

  RaftEntry entry1, entry2;

  RedisRequest req;
  RedisRequest req2;
  RaftServer srv;
  RaftTerm term;
};


TEST_F(Raft_Journal, T1) {
{
  RaftJournal journal(dbpath);

  srv = { "localhost", 1234 };

  ASSERT_EQ(journal.getCurrentTerm(), 0);
  ASSERT_EQ(journal.getLogSize(), 1);
  ASSERT_EQ(journal.getClusterID(), clusterID);
  ASSERT_EQ(journal.getCommitIndex(), 0);

  ASSERT_TRUE(journal.setCurrentTerm(2, srv));
  ASSERT_EQ(journal.getCurrentTerm(), 2);
  ASSERT_FALSE(journal.setCurrentTerm(1, srv));

  srv = {"server2", 2345 };
  ASSERT_FALSE(journal.setCurrentTerm(2, srv));

  srv = { "", 0 };
  ASSERT_TRUE(journal.setCurrentTerm(3, srv));

  srv = { "server3", 89 };
  ASSERT_TRUE(journal.setCurrentTerm(3, srv));
  srv = { "server4", 89 };
  ASSERT_FALSE(journal.setCurrentTerm(3, srv));
  ASSERT_FALSE(journal.setCurrentTerm(2, srv));

  ASSERT_EQ(journal.getNodes(), nodes);

  entry1 = RaftEntry(2, "set", "abc", "123");

  ASSERT_TRUE(journal.append(1, entry1));
  ASSERT_OK(journal.fetch(1, entry2));
  ASSERT_EQ(entry1, entry2);
  ASSERT_TRUE(journal.matchEntries(1, 2));

  ASSERT_THROW(journal.setCommitIndex(2), FatalException);

  entry1 = RaftEntry(4, "set", "qwerty", "asdf");
  ASSERT_FALSE(journal.append(2, entry1));

  entry1.term = 2;
  ASSERT_TRUE(journal.append(2, entry1));
  ASSERT_TRUE(journal.matchEntries(2, 2));
  journal.setCommitIndex(2);

  entry1 = RaftEntry(1, "set", "123", "456");
  req = entry1.request;

  ASSERT_FALSE(journal.append(3, entry1));
  ASSERT_TRUE(journal.setCurrentTerm(4, srv));

  entry1.term = 4;
  ASSERT_TRUE(journal.append(3, entry1));
  ASSERT_TRUE(journal.matchEntries(3, 4));
  ASSERT_EQ(journal.getLogSize(), 4);

  ASSERT_EQ(journal.getMembership().observers, observers);
}
{
  RaftJournal journal(dbpath);
  ASSERT_EQ(journal.getCommitIndex(), 2);
  ASSERT_EQ(journal.getLogSize(), 4);
  ASSERT_EQ(journal.getNodes(), nodes);
  ASSERT_EQ(journal.getCurrentTerm(), 4);
  ASSERT_EQ(journal.getClusterID(), clusterID);
  ASSERT_EQ(journal.getVotedFor(), srv);

  journal.fetch_or_die(3, entry1);
  ASSERT_EQ(entry1.term, 4);
  ASSERT_EQ(entry1.request, req);

  entry1.term = 3;
  ASSERT_FALSE(journal.append(4, entry1));
  ASSERT_EQ(journal.getLogSize(), 4);

  // try to remove committed entry
  ASSERT_THROW(journal.removeEntries(2), FatalException);

  ASSERT_FALSE(journal.removeEntries(5));
  ASSERT_TRUE(journal.removeEntries(3));
  ASSERT_FALSE(journal.removeEntries(3));

  ASSERT_EQ(journal.getLogSize(), 3);
  ASSERT_NOTFOUND(journal.fetch(3, entry2));
  ASSERT_NOTFOUND(journal.fetch(4, entry2));

  req = { "set", "qwerty", "asdf" };
  journal.fetch_or_die(2, entry2);
  ASSERT_EQ(entry2.term, 2);
  ASSERT_EQ(entry2.request, req);

  for(size_t i = 0; i < testreqs.size(); i++) {
    RaftEntry tempEntry;
    tempEntry.term = 4;
    tempEntry.request = testreqs[i];

    ASSERT_TRUE(journal.append(3+i, tempEntry)) << i;
  }

  ASSERT_THROW(journal.trimUntil(4), FatalException); // commit index is 2
  ASSERT_THROW(journal.trimUntil(3), FatalException);

  journal.trimUntil(2);
  journal.trimUntil(2); // no op

  ASSERT_EQ(journal.getLogStart(), 2);

  RaftEntry tmp;
  ASSERT_NOTFOUND(journal.fetch(0, tmp));
  ASSERT_NOTFOUND(journal.fetch(1, tmp));
  ASSERT_OK(journal.fetch(2, tmp));

  journal.setCommitIndex(3);

  journal.trimUntil(3);
  ASSERT_NOTFOUND(journal.fetch(2, tmp));
  ASSERT_OK(journal.fetch(3, tmp));
  ASSERT_THROW(journal.trimUntil(4), FatalException); // commit index is 3

  RaftJournal::Iterator it = journal.getIterator(2, true);
  ASSERT_FALSE(it.valid());

  it = journal.getIterator(2, false);
  ASSERT_TRUE(it.valid());
  ASSERT_EQ(it.getCurrentIndex(), 3);

  journal.setCommitIndex(6);
  journal.trimUntil(5);

  ASSERT_NOTFOUND(journal.fetch(4, tmp));
  ASSERT_EQ(journal.getLogStart(), 5);


  LogIndex size = 3+testreqs.size();
  ASSERT_EQ(journal.getLogSize(), size);

  // add and remove observers
  std::string err;

  observers.emplace_back("observer1", 123);
  ASSERT_TRUE(journal.addObserver(4, observers[0], err));
  ASSERT_EQ(journal.getMembership().observers, observers);

  observers.emplace_back("observer2", 345);

  // previous membership change not committed yet, cannot add another
  ASSERT_FALSE(journal.addObserver(4, observers[1], err));
  ASSERT_EQ(journal.getMembership().observers.size(), 1u);
  ASSERT_EQ(journal.getMembership().observers[0], observers[0]);

  ASSERT_EQ(journal.getLogSize(), size+1);
  ASSERT_OK(journal.fetch(size, tmp));
  ASSERT_EQ(tmp.request[0], "JOURNAL_UPDATE_MEMBERS");

  ASSERT_TRUE(journal.setCommitIndex(size));

  // try to add the same observer again
  ASSERT_FALSE(journal.addObserver(4, observers[0], err));
  // try to add an existing node as observer
  ASSERT_FALSE(journal.addObserver(4, nodes[1], err));
  // try to remove non-existing nodes
  ASSERT_FALSE(journal.removeMember(4, observers[1], err));
  ASSERT_FALSE(journal.removeMember(4, RaftServer("asdfad", 13), err));
  // ASSERT_FALSE(journal.removeObserver(4, nodes[2], err));

  // try to add an observer again, after committing the previous membership epoch
  ASSERT_TRUE(journal.addObserver(4, observers[1], err));
  ASSERT_EQ(journal.getMembership().observers, observers);

  // roll-back previous membership change
  ASSERT_TRUE(journal.removeEntries(size+1));
  ASSERT_EQ(journal.getMembership().observers.size(), 1u);
  ASSERT_EQ(journal.getMembership().observers[0], observers[0]);

  // add it again, and commit
  ASSERT_TRUE(journal.addObserver(4, observers[1], err));
  ASSERT_EQ(journal.getMembership().observers, observers);
  ASSERT_TRUE(journal.setCommitIndex(size+1));
  ASSERT_EQ(journal.getMembership().observers, observers);

  ASSERT_TRUE(journal.removeMember(4, observers[0], err));
  observers.erase(observers.begin());
  ASSERT_EQ(journal.getMembership().observers, observers);
  ASSERT_TRUE(journal.setCommitIndex(size+2));

  ASSERT_TRUE(journal.removeMember(4, observers[0], err));
  ASSERT_TRUE(journal.getMembership().observers.empty());

  // roll-back
  ASSERT_TRUE(journal.removeEntries(size+3));
  ASSERT_EQ(journal.getMembership().observers, observers);

  // push a regular entry, just because

  entry1 = RaftEntry(4, "set", "regular_entry", "just_because");
  ASSERT_TRUE(journal.append(size+3, entry1));

  // remove the last observer again
  ASSERT_TRUE(journal.removeMember(4, observers[0], err));
  ASSERT_TRUE(journal.getMembership().observers.empty());
  ASSERT_TRUE(journal.setCommitIndex(size+4));
  ASSERT_TRUE(journal.getMembership().observers.empty());

  // add two observers, promote the first
  ASSERT_TRUE(journal.addObserver(4, observers[0], err));
  ASSERT_TRUE(journal.setCommitIndex(size+5));

  observers.emplace_back("observer3", 789);
  ASSERT_TRUE(journal.addObserver(4, observers[1], err));
  ASSERT_TRUE(journal.setCommitIndex(size+6));
  ASSERT_EQ(journal.getMembership().observers, observers);

  observers.emplace_back("observer4", 1111);

  ASSERT_FALSE(journal.promoteObserver(4, observers[2], err));
  ASSERT_TRUE(journal.promoteObserver(4, observers[1], err));
  ASSERT_EQ(journal.getMembership().observers.size(), 1u);
  ASSERT_EQ(journal.getMembership().nodes.size(), 4u);
  ASSERT_EQ(journal.getMembership().observers[0], observers[0]);
  ASSERT_EQ(journal.getMembership().nodes[3], observers[1]);

  ASSERT_TRUE(journal.setCommitIndex(size+7));
  ASSERT_TRUE(journal.removeMember(4, nodes[0], err));
  ASSERT_EQ(journal.getMembership().nodes.size(), 3u);
  ASSERT_EQ(journal.getMembership().nodes[0], nodes[1]);
  ASSERT_TRUE(journal.setCommitIndex(size+8));

  // try to update members by appending a journal entry which has a wrong clusterID
  // verify it is ignored
  RaftMembership originalMembership = journal.getMembership();
  nodes = originalMembership.nodes;
  observers = originalMembership.observers;
  observers.emplace_back("some_node", 567);

  ASSERT_TRUE(journal.append(journal.getLogSize(), RaftEntry(4, "JOURNAL_UPDATE_MEMBERS", RaftMembers(nodes, observers).toString(), "wrong_cluster_id")));
  ASSERT_EQ(journal.getMembership(), originalMembership);
}
}
