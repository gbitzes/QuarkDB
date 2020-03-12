// ----------------------------------------------------------------------
// File: replication.cc
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
#include "raft/RaftReplicator.hh"
#include "raft/RaftTalker.hh"
#include "raft/RaftTimeouts.hh"
#include "raft/RaftCommitTracker.hh"
#include "raft/RaftReplicator.hh"
#include "raft/RaftConfig.hh"
#include "raft/RaftTrimmer.hh"
#include "utils/ParseUtils.hh"
#include "storage/ConsistencyScanner.hh"
#include "Configuration.hh"
#include "QuarkDBNode.hh"
#include "../test-utils.hh"
#include "RedisParser.hh"
#include <gtest/gtest.h>
#include <qclient/QClient.hh>
#include <qclient/shared/Communicator.hh>
#include <qclient/shared/CommunicatorListener.hh>
#include <qclient/pubsub/Subscriber.hh>
#include <qclient/pubsub/Message.hh>
#include "utils/AssistedThread.hh"
#include "../test-reply-macros.hh"

using namespace quarkdb;
class Replication : public TestCluster3NodesFixture {};
class CommunicatorTest : public TestCluster3NodesFixture {};
class Membership : public TestCluster5NodesFixture {};
class SingleNodeInitially : public TestCluster10Nodes1InitialFixture {};

TEST_F(Replication, entries_50k_with_follower_loss) {
  Connection::setPhantomBatchLimit(1);

  // let's get this party started
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));
  int leaderID = getLeaderID();

  // push lots of updates
  const int64_t NENTRIES = 50000;
  for(size_t i = 0; i < NENTRIES; i++) {
    tunnel(leaderID)->exec("set", SSTR("key-" << i), SSTR("value-" << i));
  }

  int victimFollower = (leaderID+1)%3;
  int activeFollower = (leaderID+2)%3;

  // verify the leader has started replicating some of the entries already
  RETRY_ASSERT_TRUE(journal(victimFollower)->getCommitIndex() > 5000);

  // bring down one of the followers, ensure replication is not complete
  spindown(victimFollower);
  ASSERT_TRUE(journal(victimFollower)->getLogSize() < NENTRIES);

  // ensure that eventually, the other follower gets all entries
  RETRY_ASSERT_TRUE(journal(activeFollower)->getLogSize() >= NENTRIES+2);
  ASSERT_TRUE(journal(leaderID)->getLogSize() >= NENTRIES+2);

  RETRY_ASSERT_TRUE(stateMachine(activeFollower)->getLastApplied() >= NENTRIES+1);
  RETRY_ASSERT_TRUE(stateMachine(leaderID)->getLastApplied() >= NENTRIES+1);
}

TEST_F(Replication, lease_expires_under_load) {
  Connection::setPhantomBatchLimit(1);

  // only nodes #0 and #1 are active
  spinup(0); spinup(1);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1));
  int leaderID = getLeaderID();
  int followerID = (getLeaderID()+1) % 2;

  // push lots of updates
  const int64_t NENTRIES = 50000;
  for(size_t i = 0; i < NENTRIES; i++) {
    tunnel(leaderID)->exec("set", SSTR("key-" << i), SSTR("value-" << i));
  }

  // verify the leader has started replicating some of the entries already
  RETRY_ASSERT_TRUE(journal(followerID)->getCommitIndex() > 5000);

  // bring down one the follower, ensure replication is not complete
  spindown(followerID);
  ASSERT_TRUE(journal(followerID)->getLogSize() < NENTRIES);

  // ensure the connection doesn't hang
  std::future<redisReplyPtr> reply = tunnel(leaderID)->exec("ping");
  tunnel(leaderID)->exec("set", "random evil intertwined", "write");
  ASSERT_TRUE(reply.wait_for(std::chrono::seconds(25)) == std::future_status::ready);
  ASSERT_REPLY(reply, "PONG");
  ASSERT_REPLY(tunnel(leaderID)->exec("ping", "abc123"), "abc123");
}

TEST_F(Replication, node_has_committed_entries_no_one_else_has_ensure_it_vetoes) {
  // node #0 has committed entries that no other node has. The node should
  // veto any attempts of election, so that only itself can win this election.

  ASSERT_TRUE(state(0)->observed(5, {}));
  ASSERT_TRUE(state(1)->observed(5, {}));
  ASSERT_TRUE(state(2)->observed(5, {}));

  // add a few requests to the log
  ASSERT_TRUE(journal()->append(1, RaftEntry(3, testreqs[0])));
  ASSERT_TRUE(journal()->append(2, RaftEntry(4, testreqs[1])));
  ASSERT_TRUE(journal()->append(3, RaftEntry(5, testreqs[2])));

  // commit all of them
  ASSERT_TRUE(journal()->setCommitIndex(3));

  // Here, timeouts are really important, as the veto message must go through
  // in time. Prepare the DBs before spinning up.
  prepare(0); prepare(1); prepare(2);

  // node #0 must win
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));
  ASSERT_EQ(state(0)->getSnapshot()->status, RaftStatus::LEADER);
}

TEST_F(Replication, connection_shuts_down_before_all_replies_arrive) {
  Connection::setPhantomBatchLimit(1);

  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));
  int leaderID = getLeaderID();

  const int64_t NENTRIES = 10000;
  for(size_t repeat = 0; repeat < 3; repeat++) {
    // push lots of updates
    for(size_t i = 0; i < NENTRIES; i++) {
      tunnel(leaderID)->exec("set", SSTR("key-" << i), SSTR("value-" << i));
    }

    killTunnel(leaderID);
  }

  for(size_t i = 0; i < NENTRIES; i++) {
    tunnel(leaderID)->exec("set", SSTR("key-" << i), SSTR("value-" << i));
  }
  tunnel(leaderID)->exec("ping").get();
  ASSERT_TRUE(leaderID == getLeaderID());
  ASSERT_TRUE(journal()->getCommitIndex() > NENTRIES);
  // if we haven't crashed or gotten hung by now, we're grand
}

// blindly generate load, ignore any errors
static void generateLoad(qclient::QClient *qcl, std::string prefix, quarkdb::ThreadAssistant &assistant) {
  int counter = 0;
  while(!assistant.terminationRequested()) {
    qcl->exec("set", SSTR(prefix <<  "-key-" << counter), SSTR(prefix << "value-" << counter));
    counter++;
  }
  qdb_info("Stopping load generation towards '" << prefix << "', waiting on pending replies");
  qcl->exec("ping").get();
  qdb_info("Shutting down load generator towards '" << prefix << "'");
}

TEST_F(Replication, load_during_election) {
  Connection::setPhantomBatchLimit(1);

  // add backpressure to load-generating threads
  qclient::Options opts0 = makeNoRedirectOptions();
  opts0.backpressureStrategy = qclient::BackpressureStrategy::RateLimitPendingRequests(1024);
  qclient::QClient qcl0(myself(0).hostname, myself(0).port, std::move(opts0));

  qclient::Options opts1 = makeNoRedirectOptions();
  opts1.backpressureStrategy = qclient::BackpressureStrategy::RateLimitPendingRequests(1024);
  qclient::QClient qcl1(myself(1).hostname, myself(1).port, std::move(opts1));

  qclient::Options opts2 = makeNoRedirectOptions();
  opts2.backpressureStrategy = qclient::BackpressureStrategy::RateLimitPendingRequests(1024);
  qclient::QClient qcl2(myself(2).hostname, myself(2).port, std::move(opts2));

  // let's be extra evil and start generating load even before the nodes start up
  quarkdb::AssistedThread t1(generateLoad, &qcl0, "node0");
  quarkdb::AssistedThread t2(generateLoad, &qcl1, "node1");
  quarkdb::AssistedThread t3(generateLoad, &qcl2, "node2");

  // start the cluster
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  // terminate once we reach a decent number of writes
  int leaderID = getLeaderID();
  RETRY_ASSERT_TRUE(journal(leaderID)->getCommitIndex() > 20000);
  ASSERT_TRUE(leaderID == getLeaderID());

  t1.stop();
  t2.stop();
  t3.stop();
}

static void assert_linearizability(std::future<redisReplyPtr> &future, std::string expectedValue, std::atomic<int64_t> &responses, std::atomic<int64_t> &violations) {
  redisReplyPtr reply = future.get();
  if(reply->type != REDIS_REPLY_STRING) return;

  responses++;
  std::string receivedValue = std::string(reply->str, reply->len);
  if(expectedValue != receivedValue) {
    violations++;
    qdb_critical("Linearizability violation. Received " << quotes(receivedValue) << ", expected: " << quotes(expectedValue));
  }
}

// Given an endpoint, try to read a key again and again and again.
// If we get ERR or MOVED, no problem.
// If we get a response other than expectedValue, linearizability has been violated.
static void obsessiveReader(qclient::QClient *qcl, std::string key, std::string expectedValue, std::atomic<int64_t> &responses, std::atomic<int64_t> &violations, quarkdb::ThreadAssistant &assistant) {
  std::list<std::future<redisReplyPtr>> futures;

  qdb_info("Issuing a flood of reads for key " << quotes(key));

  while(!assistant.terminationRequested()) {
    futures.emplace_back(qcl->exec("get", key));

    while(futures.size() >= 1000) {
      assert_linearizability(futures.front(), expectedValue, responses, violations);
      futures.pop_front();
    }
  }

  while(futures.size() != 0) {
    assert_linearizability(futures.front(), expectedValue, responses, violations);
    futures.pop_front();
  }
}

TEST_F(Replication, linearizability_during_transition) {
  // start the cluster
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));
  int leaderID = getLeaderID();

  // activate full consistency scan every second, across every node
  ASSERT_REPLY(tunnel(leaderID)->exec("config_set", ConsistencyScanner::kConfigurationKey, "1"), "OK");

  std::vector<std::future<redisReplyPtr>> futures;

  // Issue a bunch of writes, all towards the same key
  const int nWrites = 10000;
  for(size_t i = 0; i <= nWrites; i++) {
    futures.push_back(tunnel(leaderID)->exec("set", "key", SSTR("value-" << i)));
  }

  // Receive responses
  for(size_t i = 0; i < futures.size(); i++) {
    ASSERT_REPLY(futures[i], "OK");
  }

  // our followers..
  int node1 = (leaderID + 1) % 3;
  int node2 = (leaderID + 2) % 3;

  // start reading "key"
  std::atomic<int64_t> responses(0), violations(0);

  quarkdb::AssistedThread reader1(obsessiveReader, tunnel(node1), "key", SSTR("value-" << nWrites), std::ref(responses), std::ref(violations));
  quarkdb::AssistedThread reader2(obsessiveReader, tunnel(node2), "key", SSTR("value-" << nWrites), std::ref(responses), std::ref(violations));

  RaftTerm firstTerm = state(leaderID)->getSnapshot()->term;

  // stop the leader
  spindown(leaderID);

  // Ensure failover happens..
  RETRY_ASSERT_NE(state(node1)->getSnapshot()->term, firstTerm);
  RETRY_ASSERT_TRUE(checkStateConsensus(node1, node2));
  int newLeaderID = getLeaderID();
  ASSERT_NE(leaderID, newLeaderID);

  // Wait until we have 1k real responses (not errors or "moved")
  RETRY_ASSERT_TRUE(responses >= 1000);

  reader1.stop(); reader2.stop();
  reader1.join(); reader2.join();

  qdb_info("After " << responses << " reads, linearizability was violated " << violations << " times.");
  ASSERT_EQ(violations, 0);

  RETRY_ASSERT_TRUE(checkFullConsensus(node1, node2));
  ASSERT_TRUE(crossCheckJournals(node1, node2));
}

void consumeListener(CommunicatorRequest &&req) {
  int64_t round = 0;
  ParseUtils::parseInt64(req.getContents().substr(6), round);
  req.sendReply(round, SSTR("reply-" << round));
}

TEST_F(CommunicatorTest, ChaosTest) {
  // start the cluster
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  qclient::Subscriber subscriber1(members(), reasonableSubscriptionOptions(true));
  qclient::Subscriber subscriber2(members(), reasonableSubscriptionOptions(true));

  qclient::Communicator communicator(&subscriber1, "comm-channel", nullptr, std::chrono::seconds(1),
    std::chrono::minutes(5));
  qclient::CommunicatorListener communicatorListener(&subscriber2, "comm-channel");

  using namespace std::placeholders;
  communicatorListener.attach(std::bind(&consumeListener, _1));

  ClusterDestabilizer destabilizer(this);

  std::vector<std::future<CommunicatorReply>> futReplies;

  for(size_t round = 0; round < 1000; round++) {
    futReplies.emplace_back(communicator.issue(SSTR("round-" << round)));
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  for(size_t i = 0; i < futReplies.size(); i++) {
    ASSERT_EQ(futReplies[i].wait_for(std::chrono::seconds(30)), std::future_status::ready) << i;

    CommunicatorReply reply = futReplies[i].get();
    ASSERT_EQ(reply.status, (int) i);
    ASSERT_EQ(reply.contents, SSTR("reply-" << i));
  }
}

TEST_F(Replication, several_transitions) {
  Connection::setPhantomBatchLimit(1);

  // Start load generation..
  quarkdb::AssistedThread t1(generateLoad, tunnel(0), "node0");
  quarkdb::AssistedThread t2(generateLoad, tunnel(1), "node1");
  quarkdb::AssistedThread t3(generateLoad, tunnel(2), "node2");

  // start the cluster
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  // Transition the leader several times, while the
  // load generators are hammering all nodes at the
  // same time. :)

  for(size_t iteration = 0; iteration < 5; iteration++) {
    std::this_thread::sleep_for(timeouts().getHigh() * 5);

    int leaderID = getLeaderID();
    int follower1 = (getLeaderID() + 1) % 3;
    int follower2 =  (getLeaderID() + 2) % 3;

    RaftTerm oldTerm = state(leaderID)->getSnapshot()->term;
    spindown(leaderID);

    RETRY_ASSERT_TRUE(oldTerm < state(follower1)->getSnapshot()->term);
    RETRY_ASSERT_TRUE(oldTerm < state(follower2)->getSnapshot()->term);
    RETRY_ASSERT_TRUE(checkStateConsensus(follower1, follower2));
    spinup(leaderID);
  }

  t1.stop(); t2.stop(); t3.stop();
  t1.join(); t2.join(); t3.join();

  RETRY_ASSERT_TRUE(checkFullConsensus(0, 1, 2));
  ASSERT_TRUE(crossCheckJournals(0, 1, 2));
}

TEST_F(Replication, FollowerLaggingBy1m) {
  Connection::setPhantomBatchLimit(1);

  spinup(0); spinup(1);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1));
  int leaderID = getLeaderID();

  std::vector<std::future<redisReplyPtr>> futs;
  for(size_t i = 0; i < 1'000'000; i++) {
    futs.emplace_back(tunnel(leaderID)->exec("set", "abc", SSTR(i)));
  }

  for(size_t i = 0; i < futs.size(); i++) {
    ASSERT_REPLY_DESCRIBE(futs[i].get(), "OK");
  }

  qdb_info("All writes processed, waiting until follower catches up...");

  spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  // bringing the lagging follower up-to-date could take a while
  RETRY_ASSERT_TRUE_20MIN(checkFullConsensus(0, 1, 2));
  ASSERT_TRUE(crossCheckJournals(0, 1, 2));
}

TEST_F(Membership, prevent_promotion_of_outdated_observer) {
  // Only start nodes 0, 1, 2 of a 5-node cluster
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));
  int leaderID = getLeaderID();

  // Remove #4 and #5
  ASSERT_REPLY(tunnel(leaderID)->exec("RAFT_REMOVE_MEMBER", myself(3).toString()), "OK");
  RETRY_ASSERT_TRUE(journal(leaderID)->getEpoch() <= journal(leaderID)->getCommitIndex());

  ASSERT_REPLY(tunnel(leaderID)->exec("RAFT_REMOVE_MEMBER", myself(4).toString()), "OK");
  RETRY_ASSERT_TRUE(journal(leaderID)->getEpoch() <= journal(leaderID)->getCommitIndex());

  // turn off one of the active nodes - now only 2 out of 3 are active
  int victim = (leaderID + 1) % 3;
  spindown(victim);

  // push lots of updates
  std::vector<std::future<redisReplyPtr>> futures;
  const int64_t NENTRIES = 50000;
  for(size_t i = 0; i < NENTRIES; i++) {
    futures.emplace_back(tunnel(leaderID)->exec("set", SSTR("key-" << i), SSTR("value-" << i)));
  }

  for(size_t i = 0; i < futures.size(); i++) {
    ASSERT_REPLY(futures[i], "OK");
  }

  // add back #3 as observer, try to promote them right away, ensure it gets
  // blocked.

  ASSERT_REPLY(tunnel(leaderID)->exec("RAFT_ADD_OBSERVER", myself(4).toString()), "OK");
  RETRY_ASSERT_TRUE(journal(leaderID)->getEpoch() <= journal(leaderID)->getCommitIndex());

  // let #4 be brought up to date, then add it as observer
  ASSERT_REPLY(tunnel(leaderID)->exec("RAFT_PROMOTE_OBSERVER", myself(4).toString()), "ERR membership update blocked, observer is not up-to-date");
  spinup(4);

  RETRY_ASSERT_EQ(journal(4)->getCommitIndex(), journal(leaderID)->getCommitIndex());
  ASSERT_REPLY(tunnel(leaderID)->exec("RAFT_PROMOTE_OBSERVER", myself(4).toString()), "OK");
}

TEST_F(Replication, no_committing_entries_from_previous_terms) {
  // Try to emulate Figure 8 of the raft paper. Certain entries are
  // present in a majority of replicas, but can still be removed as
  // conflicting. Ensure the leader never marks them as committed!

  state(0)->observed(2, {});
  state(1)->observed(2, {});
  state(2)->observed(2, {});

  for(size_t i = 1; i < 50000; i++) {
    RaftEntry entry;
    entry.term = 2;
    entry.request = make_req("set", SSTR("entry-" << i), SSTR("contents-" << i));

    ASSERT_TRUE(journal(1)->append(i, entry));
  }

  state(0)->observed(100, {});
  state(1)->observed(100, {});
  state(2)->observed(100, {});

  spinup(0); spinup(1);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1));
  ASSERT_TRUE(getLeaderID() == 1);

  // We have node #1 as leader for term > 100 - wait until it replicates
  // some of the entries, then shut it down.
  RETRY_ASSERT_TRUE(journal(0)->getLogSize() >= 100);
  spindown(1); spindown(0);

  // Ensure no entry got committed!
  ASSERT_TRUE(journal(0)->getCommitIndex() == 0);
  ASSERT_TRUE(journal(1)->getCommitIndex() == 0);

  // Make things even more interesting by starting up node #2 which
  // has more up-to-date log. :) Ensure it overwrites the rest.
  state(2)->observed(2000, {});
  RaftEntry entry;
  entry.term = 2000;
  entry.request = make_req("set", "one entry", "to rule them all");
  ASSERT_TRUE(journal(2)->append(1, entry));

  spinup(2); spinup(1);
  RETRY_ASSERT_TRUE(checkStateConsensus(1, 2));
  ASSERT_TRUE(state(1)->getSnapshot()->leader == myself(2));

  LogIndex leadershipMarker = state(2)->getSnapshot()->leadershipMarker;
  RETRY_ASSERT_EQ(journal(1)->getLogSize(), leadershipMarker+1);
  RETRY_ASSERT_EQ(journal(1)->getCommitIndex(), leadershipMarker);
  RETRY_ASSERT_EQ(journal(2)->getCommitIndex(), leadershipMarker);

  // now start node #0, too, ensure its contents are overwritten as well
  spinup(0);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));
  ASSERT_TRUE(state(0)->getSnapshot()->leader == myself(2));
  RETRY_ASSERT_EQ(journal(0)->getLogSize(), leadershipMarker+1);
  RETRY_ASSERT_EQ(journal(0)->getCommitIndex(), leadershipMarker);
}

TEST_F(Replication, TrimmingBlock) {
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));
  int leaderID = getLeaderID();

  // Set journal trim config to ridiculously low values.
  TrimmingConfig trimConfig { 2, 1 };
  EncodedConfigChange configChange = raftconfig(leaderID)->setTrimmingConfig(trimConfig, true);
  ASSERT_TRUE(configChange.error.empty());
  ASSERT_REPLY(tunnel(leaderID)->execute(configChange.request), "OK");

  // Ensure it took effect...
  for(size_t i = 0; i < 100; i++) {
    ASSERT_REPLY(tunnel(leaderID)->exec("set", SSTR("entry-" << i), SSTR("val-" << i)), "OK");
  }

  // Ensure everyone apart from the leader trimmed their journals.
  // (The replicator might have blocked such aggressive trimming)
  LogIndex logSize = journal(leaderID)->getLogSize();

  RETRY_ASSERT_EQ(journal((leaderID+1) % 3)->getLogStart(), (logSize - 3));
  RETRY_ASSERT_EQ(journal((leaderID+2) % 3)->getLogStart(), (logSize - 3));

  // Put a trimming block in place for one of the followers.
  RaftTrimmingBlock trimmingBlock(*trimmer((leaderID+1)%3), 0);

  for(size_t i = 0; i < 100; i++) {
    ASSERT_REPLY(tunnel(leaderID)->exec("set", SSTR("entry2-" << i), SSTR("val2-" << i)), "OK");
  }

  std::this_thread::sleep_for(std::chrono::seconds(5));
  ASSERT_TRUE(journal((leaderID+1) % 3)->getLogStart() == (logSize - 3));

  LogIndex newLogSize = journal(leaderID)->getLogSize();
  RETRY_ASSERT_EQ(journal((leaderID+2) % 3)->getLogStart(), (newLogSize - 3));

  // Lift block, ensure journal is trimmed
  trimmingBlock.enforce(150);
  RETRY_ASSERT_EQ(journal((leaderID+1) % 3)->getLogStart(), 149);

  trimmingBlock.enforce(151);
  RETRY_ASSERT_EQ(journal((leaderID+1) % 3)->getLogStart(), 150);

  trimmingBlock.enforce(155);
  RETRY_ASSERT_EQ(journal((leaderID+1) % 3)->getLogStart(), 154);

  trimmingBlock.enforce(173);
  RETRY_ASSERT_EQ(journal((leaderID+1) % 3)->getLogStart(), 172);

  trimmingBlock.lift();
  RETRY_ASSERT_EQ(journal((leaderID+1) % 3)->getLogStart(), (newLogSize - 3));
}

TEST_F(Replication, EnsureEntriesBeingReplicatedAreNotTrimmed) {
  Connection::setPhantomBatchLimit(1);

  spinup(0); spinup(1);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1));
  int leaderID = getLeaderID();

  std::vector<std::future<redisReplyPtr>> replies;
  for(size_t i = 0; i < 10000; i++) {
    replies.emplace_back(tunnel(leaderID)->exec("set", SSTR("entry-" << i), SSTR("contents-" << i)));
  }

  for(size_t i = 0; i < 10000; i++) {
    ASSERT_REPLY(replies[i], "OK");
  }

  // Spin up node #2
  spinup(2);
  RETRY_ASSERT_TRUE(journal(2)->getLogSize() >= 10);

  // Set an insane trimming limit.
  TrimmingConfig trimConfig { 2, 1 };
  EncodedConfigChange configChange = raftconfig(leaderID)->setTrimmingConfig(trimConfig, true);
  ASSERT_TRUE(configChange.error.empty());
  ASSERT_REPLY(tunnel(leaderID)->execute(configChange.request), "OK");

  // Ensure the replicator doesn't blow up
  while(journal(2)->getLogSize() != journal(leaderID)->getLogSize()) {
    ASSERT_FALSE(trimmer(leaderID)->canTrimUntil(journal(2)->getLogSize()));
  }

  // Ensure contents are OK.
  for(size_t i = 0; i < 10000; i++) {
    std::string value;
    ASSERT_TRUE(stateMachine(leaderID)->get(SSTR("entry-" << i), value).ok());
    ASSERT_EQ(value, SSTR("contents-" << i));
  }

  // Ensure journals are eventually trimmed.
  RETRY_ASSERT_EQ(journal(0)->getLogStart(), 10000);
  RETRY_ASSERT_EQ(journal(1)->getLogStart(), 10000);
  RETRY_ASSERT_EQ(journal(2)->getLogStart(), 10000);

  ASSERT_TRUE(trimmer(0)->canTrimUntil(9999));
  ASSERT_TRUE(trimmer(1)->canTrimUntil(9999));
  ASSERT_TRUE(trimmer(2)->canTrimUntil(9999));

  ASSERT_FALSE(trimmer(leaderID)->canTrimUntil(10001));
}

TEST_F(SingleNodeInitially, Multi) {
  spinup(0);
  RETRY_ASSERT_EQ(state(0)->getSnapshot()->status, RaftStatus::LEADER);

  std::deque<qclient::EncodedRequest> multi1;
  multi1.emplace_back(qclient::EncodedRequest::make("set", "my-awesome-counter", "1"));
  multi1.emplace_back(qclient::EncodedRequest::make("set", "other-counter", "12345"));
  multi1.emplace_back(qclient::EncodedRequest::make("get", "other-counter"));
  multi1.emplace_back(qclient::EncodedRequest::make("get", "my-awesome-counter"));

  ASSERT_EQ(
    qclient::describeRedisReply(tunnel(0)->execute(std::move(multi1)).get()),
    "1) OK\n"
    "2) OK\n"
    "3) \"12345\"\n"
    "4) \"1\"\n"
  );
}

TEST_F(SingleNodeInitially, SingleNodeRaftMode) {
  spinup(0);
  RETRY_ASSERT_EQ(state(0)->getSnapshot()->status, RaftStatus::LEADER);

  std::vector<std::future<redisReplyPtr>> replies;
  for(size_t i = 0; i < 100; i++) {
    replies.emplace_back(tunnel(0)->exec("set", SSTR("entry-" << i), SSTR("contents-" << i)));
  }

  for(size_t i = 0; i < 100; i++) {
    replies.emplace_back(tunnel(0)->exec("get", SSTR("entry-" << i)));
  }

  for(size_t i = 0; i < 100; i++) {
    ASSERT_REPLY(replies[i], "OK");
  }

  for(size_t i = 100; i < 200; i++) {
    ASSERT_REPLY(replies[i], SSTR("contents-" << i-100));
  }
}

TEST_F(SingleNodeInitially, BuildClusterFromSingle) {
  spinup(0);
  RETRY_ASSERT_EQ(state(0)->getSnapshot()->status, RaftStatus::LEADER);

  std::vector<std::future<redisReplyPtr>> replies;
  for(size_t i = 0; i < 100; i++) {
    replies.emplace_back(tunnel(0)->exec("set", SSTR("entry-" << i), SSTR("contents-" << i)));
  }

  for(size_t i = 0; i < 100; i++) {
    ASSERT_REPLY(replies[i], "OK");
  }

  // spinup node #1, add to cluster
  spinup(1);

  ASSERT_REPLY(tunnel(0)->exec("RAFT_ADD_OBSERVER", myself(1).toString()), "OK");
  RETRY_ASSERT_TRUE(journal(0)->getEpoch() <= journal(0)->getCommitIndex());

  // promote
  RETRY_ASSERT_TRUE(
    qclient::describeRedisReply(tunnel(0)->exec("RAFT_PROMOTE_OBSERVER", myself(1).toString()).get())
    == "OK"
  );

  // More writes
  replies.clear();
  for(size_t i = 100; i < 200; i++) {
    replies.emplace_back(tunnel(0)->exec("set", SSTR("entry-" << i), SSTR("contents-" << i)));
  }

  for(size_t i = 0; i < 100; i++) {
    ASSERT_REPLY(replies[i], "OK");
  }

  // Switch over leadership to #1
  while(true) {
    RETRY_ASSERT_TRUE(checkStateConsensus(0, 1));
    int leaderID = getLeaderID();
    if(leaderID == 1) break;
    tunnel(1)->exec("raft-attempt-coup").get();

    std::this_thread::sleep_for(timeouts().getLow());
  }

  // commit a write
  ASSERT_REPLY(tunnel(1)->exec("set", "aaa", "123"), "OK");

  // get a previous read
  ASSERT_REPLY(tunnel(1)->exec("get", "entry-100"), "contents-100");

  // remove #0
  ASSERT_REPLY(tunnel(1)->exec("raft-remove-member", myself(0).toString()), "OK");

  // commit one more write
  ASSERT_REPLY(tunnel(1)->exec("set", "bbb", "123"), "OK");

  // read
  ASSERT_REPLY(tunnel(1)->exec("get", "entry-5"), "contents-5");
}
