// ----------------------------------------------------------------------
// File: background-flusher.cc
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
#include "Connection.hh"
#include <qclient/BackgroundFlusher.hh>
#include <qclient/RocksDBPersistency.hh>
#include "../test-utils.hh"
#include "RedisParser.hh"
#include "../test-reply-macros.hh"
#include <gtest/gtest.h>

using namespace quarkdb;

using namespace quarkdb;
#define ASSERT_OK(msg) ASSERT_TRUE(msg.ok())
class Background_Flusher : public TestCluster3NodesFixture {};

TEST_F(Background_Flusher, basic_sanity) {
  Connection::setPhantomBatchLimit(1);

  // start our cluster as usual
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  int leaderID = getLeaderID();
  int follower = (leaderID + 1) % 3;

  qclient::Notifier dummyNotifier;
  ASSERT_EQ(system("rm -rf /tmp/quarkdb-tests-flusher"), 0);

  qclient::Options opts;
  opts.handshake = makeQClientHandshake();
  qclient::BackgroundFlusher flusher(qclient::Members(myself(follower).hostname, myself(follower).port),
    std::move(opts), dummyNotifier, new qclient::RocksDBPersistency("/tmp/quarkdb-tests-flusher")
  );

  const int nentries = 10000;
  for(size_t i = 0; i <= nentries; i++) {
    flusher.pushRequest({"set", "key", SSTR("value-" << i)});
  }

  RETRY_ASSERT_EQ(flusher.size(), 0u);
  RETRY_ASSERT_TRUE(checkFullConsensus(0, 1, 2));
  ASSERT_TRUE(checkValueConsensus("key", SSTR("value-" << nentries), 0, 1, 2));

  // verify that every single request has been recorded
  LogIndex lastEntry = journal(leaderID)->getLogSize() - 1;
  LogIndex firstEntry = lastEntry - nentries;

  for(LogIndex index = lastEntry; index >= firstEntry; index--) {
    int64_t value = index - firstEntry;
    // -1: We don't care about the entry term.
    ASSERT_TRUE(validateSingleEntry(index, -1, make_req("set", "key", SSTR("value-" << value)), 0, 1, 2));
  }
}

TEST_F(Background_Flusher, with_transition) {
  // start our cluster as usual
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  int leaderID = getLeaderID();
  int follower1 = (getLeaderID() + 1) % 3;
  int follower2 = (getLeaderID() + 2) % 3;

  qclient::Members members;
  members.push_back(myself(0).hostname, myself(0).port);
  members.push_back(myself(1).hostname, myself(1).port);
  members.push_back(myself(2).hostname, myself(2).port);

  qclient::Notifier dummyNotifier;
  ASSERT_EQ(system("rm -rf /tmp/quarkdb-tests-flusher"), 0);
  qclient::Options opts;
  opts.handshake = makeQClientHandshake();

  qclient::BackgroundFlusher flusher(members, std::move(opts), dummyNotifier,
    new qclient::RocksDBPersistency("/tmp/quarkdb-tests-flusher")
  );

  const int nentries = 10000;
  for(size_t i = 0; i <= nentries/2; i++) {
    flusher.pushRequest({"set", SSTR("key-" << i), SSTR("value-" << i)});
  }

  RETRY_ASSERT_TRUE(flusher.size() <= 2500u);
  spindown(leaderID);

  for(size_t i = nentries/2 + 1; i <= nentries; i++) {
    flusher.pushRequest({"set", SSTR("key-" << i), SSTR("value-" << i)});
  }

  RETRY_ASSERT_EQ(flusher.size(), 0u);
  RETRY_ASSERT_TRUE(checkFullConsensus(follower1, follower2));
  for(size_t i = 0; i <= nentries; i++) {
    ASSERT_TRUE(checkValueConsensus(SSTR("key-" << i), SSTR("value-" << i), follower1, follower2));
  }
}

TEST_F(Background_Flusher, persistency) {
  // start our cluster as usual
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  int leaderID = getLeaderID();
  int follower = (leaderID + 1) % 3;

  qclient::Notifier dummyNotifier;

  ASSERT_EQ(system("rm -rf /tmp/quarkdb-tests-flusher"), 0);
  qclient::Options opts;
  opts.handshake = makeQClientHandshake();
  std::unique_ptr<qclient::BackgroundFlusher> flusher(
    new qclient::BackgroundFlusher(qclient::Members(myself(follower).hostname, myself(follower).port), std::move(opts), dummyNotifier, new qclient::RocksDBPersistency("/tmp/quarkdb-tests-flusher"))
  );

  // queue entries
  const int nentries = 10000;
  for(size_t i = 0; i <= nentries; i++) {
    flusher->pushRequest({"set", SSTR("key-" << i), SSTR("value-" << i)});
  }

  ASSERT_GT(flusher->size(), 0u);

  // stop the flusher, recover contents from persistency layer
  flusher.reset();
  opts = {};
  opts.handshake = makeQClientHandshake();
  flusher.reset(new qclient::BackgroundFlusher(qclient::Members(myself(follower).hostname, myself(follower).port), std::move(opts), dummyNotifier, new qclient::RocksDBPersistency("/tmp/quarkdb-tests-flusher")));
  ASSERT_GT(flusher->size(), 0u);

  RETRY_ASSERT_EQ(flusher->size(), 0u);
  RETRY_ASSERT_EQ(stateMachine(follower)->getLastApplied(), stateMachine(leaderID)->getLastApplied());
  for(size_t i = 0; i <= nentries; i++) {
    ASSERT_TRUE(checkValueConsensus(SSTR("key-" << i), SSTR("value-" << i), leaderID, follower));
  }
}

TEST(RocksDBPersistency, basic_sanity) {
  ASSERT_EQ(system("rm -rf /tmp/quarkdb-tests-flusher"), 0);
  std::unique_ptr<qclient::RocksDBPersistency> persistency(new qclient::RocksDBPersistency("/tmp/quarkdb-tests-flusher"));
  ASSERT_EQ(persistency->getStartingIndex(), 0);
  ASSERT_EQ(persistency->getEndingIndex(), 0);

  persistency->record(0, {"test", "asdf", "1234"});
  ASSERT_EQ(persistency->getStartingIndex(), 0);
  ASSERT_EQ(persistency->getEndingIndex(), 1);

  persistency->record(1, {"item1", "item2", "item3"} );
  persistency->record(2, {"entry2"});

  std::vector<std::string> vec;
  ASSERT_TRUE(persistency->retrieve(2, vec));
  ASSERT_EQ(vec, make_vec("entry2"));

  ASSERT_EQ(persistency->getStartingIndex(), 0);
  ASSERT_EQ(persistency->getEndingIndex(), 3);

  persistency.reset();
  persistency.reset(new qclient::RocksDBPersistency("/tmp/quarkdb-tests-flusher"));

  ASSERT_EQ(persistency->getStartingIndex(), 0);
  ASSERT_EQ(persistency->getEndingIndex(), 3);

  persistency->pop();
  ASSERT_TRUE(persistency->retrieve(1, vec));
  ASSERT_EQ(vec, make_vec("item1", "item2", "item3"));

  ASSERT_EQ(persistency->getStartingIndex(), 1);
  ASSERT_EQ(persistency->getEndingIndex(), 3);

  persistency.reset();
  persistency.reset(new qclient::RocksDBPersistency("/tmp/quarkdb-tests-flusher"));

  ASSERT_EQ(persistency->getStartingIndex(), 1);
  ASSERT_EQ(persistency->getEndingIndex(), 3);
}
