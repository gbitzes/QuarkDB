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
    RaftJournal::ObliterateAndReinitializeJournal(dbpath, clusterID, nodes);
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

  entry1.request = { "set", "abc", "123" };
  entry1.term = 2;

  ASSERT_TRUE(journal.append(1, entry1.term, entry1.request));
  ASSERT_OK(journal.fetch(1, entry2));
  ASSERT_EQ(entry1, entry2);
  ASSERT_TRUE(journal.matchEntries(1, 2));

  ASSERT_THROW(journal.setCommitIndex(2), FatalException);
  req = { "set", "qwerty", "asdf" };
  ASSERT_FALSE(journal.append(2, 4, req));
  ASSERT_TRUE(journal.append(2, 2, req));
  ASSERT_TRUE(journal.matchEntries(2, 2));
  journal.setCommitIndex(2);

  req = { "set", "123", "456"};
  ASSERT_FALSE(journal.append(3, 1, req));
  ASSERT_TRUE(journal.setCurrentTerm(4, srv));
  ASSERT_TRUE(journal.append(3, 4, req));
  ASSERT_TRUE(journal.matchEntries(3, 4));
  ASSERT_EQ(journal.getLogSize(), 4);


  ASSERT_EQ(journal.getObservers(), observers);
  observers.emplace_back("observer1", 123);
  observers.emplace_back("observer2", 345);
  journal.setObservers(observers);
  ASSERT_EQ(journal.getObservers(), observers);
}
{
  RaftJournal journal(dbpath);
  ASSERT_EQ(journal.getCommitIndex(), 2);
  ASSERT_EQ(journal.getLogSize(), 4);
  ASSERT_EQ(journal.getNodes(), nodes);
  ASSERT_EQ(journal.getCurrentTerm(), 4);
  ASSERT_EQ(journal.getClusterID(), clusterID);
  ASSERT_EQ(journal.getVotedFor(), srv);
  ASSERT_EQ(journal.getObservers(), observers);

  journal.fetch_or_die(3, entry1);
  ASSERT_EQ(entry1.term, 4);
  ASSERT_EQ(entry1.request, req);

  ASSERT_FALSE(journal.append(4, 3, req));
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
    ASSERT_TRUE(journal.append(3+i, 4, testreqs[i])) << i;
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

  journal.setCommitIndex(6);
  journal.trimUntil(5);

  ASSERT_NOTFOUND(journal.fetch(4, tmp));
  ASSERT_EQ(journal.getLogStart(), 5);
}
}
