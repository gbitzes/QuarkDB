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

#include "raft/RaftDispatcher.hh"
#include "raft/RaftReplicator.hh"
#include "raft/RaftTalker.hh"
#include "raft/RaftTimeouts.hh"
#include "raft/RaftCommitTracker.hh"
#include "Poller.hh"
#include "test-utils.hh"
#include "RedisParser.hh"
#include <gtest/gtest.h>

using namespace quarkdb;
#define ASSERT_OK(msg) ASSERT_TRUE(msg.ok())
#define ASSERT_REPLY(reply, val) { ASSERT_NE(reply, nullptr); ASSERT_EQ(std::string(((reply))->str, ((reply))->len), val); }

class Raft_Replicator : public TestCluster3Nodes {};
class Raft_Voting : public TestCluster3Nodes {};
class Raft_Dispatcher : public TestCluster3Nodes {};
class Raft_Election : public TestCluster3Nodes {};
class Raft_Director : public TestCluster3Nodes {};
class Raft_CommitTracker : public TestCluster3Nodes {};

TEST_F(Raft_Replicator, no_replication_on_myself) {
  ASSERT_TRUE(state()->observed(2, {}));
  ASSERT_TRUE(state()->becomeCandidate(2));
  ASSERT_TRUE(state()->ascend(2));
  ASSERT_THROW(replicator()->launch(myself(), state()->getSnapshot()), FatalException);
}

TEST_F(Raft_Replicator, only_leader_can_launch_replicator) {
  ASSERT_THROW(replicator()->launch(nodes()[1], state()->getSnapshot()), FatalException);
}

TEST_F(Raft_Replicator, verify_sane_snapshot_term) {
  ASSERT_TRUE(state()->observed(2, {}));
  ASSERT_TRUE(state()->becomeCandidate(2));
  ASSERT_TRUE(state()->ascend(2));

  // trying to replicate for a term in the future
  RaftStateSnapshot snapshot = state()->getSnapshot();
  snapshot.term = 3;
  ASSERT_THROW(replicator()->launch(nodes()[1], snapshot), FatalException);

  // stale term - this can naturally happen, so it is not an exception
  ASSERT_TRUE(state()->observed(4, {}));
  ASSERT_FALSE(replicator()->launch(nodes()[1], snapshot));
}

TEST_F(Raft_Replicator, do_simple_replication) {
  // node #0 will replicate its log to node #1
  ASSERT_TRUE(state(0)->observed(2, {}));
  ASSERT_TRUE(state(0)->becomeCandidate(2));
  ASSERT_TRUE(state(0)->ascend(2));

  // add an inconsistent journal entry to #1, just for fun
  ASSERT_TRUE(journal(1)->append(1, 0, make_req("supposed", "to", "be", "removed")));

  ASSERT_EQ(state(1)->getCurrentTerm(), 0);

  // activate poller for #1
  poller(1);

  // launch!
  ASSERT_TRUE(replicator(0)->launch(myself(1), state(0)->getSnapshot()));

  // populate #0's journal
  for(size_t i = 0; i < testreqs.size(); i++) {
    ASSERT_TRUE(journal(0)->append(i+2, 2, testreqs[i]));
  }

  // verify #1 recognized #0 as leader and that replication was successful
  RETRY_ASSERT_TRUE(journal(1)->getLogSize() == (int64_t) testreqs.size()+2);

  RaftStateSnapshot snapshot = state(1)->getSnapshot();
  ASSERT_EQ(snapshot.term, 2);
  ASSERT_EQ(snapshot.leader, myself(0));

  for(size_t i = 0; i < testreqs.size(); i++) {
    RaftEntry entry;
    ASSERT_TRUE(dispatcher(1)->fetch(i+2, entry));
    ASSERT_EQ(entry.term, 2);
    ASSERT_EQ(entry.request, testreqs[i]);
  }
}

TEST_F(Raft_Replicator, test_replication_with_empty_journals) {
  // node #0 will do replication to #1, but with a journal that only contains
  // 1 entry.

  ASSERT_TRUE(state(0)->observed(2, {}));
  ASSERT_TRUE(state(0)->becomeCandidate(2));
  ASSERT_TRUE(state(0)->ascend(2));

  // active poller for #1
  poller(1);

  // launch
  ASSERT_TRUE(replicator(0)->launch(myself(1), state(0)->getSnapshot()));

  // verify everything's sane
  RETRY_ASSERT_TRUE(state(1)->getSnapshot().leader == myself(0));
  RaftStateSnapshot snapshot = state(1)->getSnapshot();
  ASSERT_EQ(snapshot.term, 2);
  ASSERT_EQ(snapshot.leader, myself(0));

  RaftEntry entry;
  journal(1)->fetch_or_die(1, entry);
  ASSERT_EQ(entry.request, make_req("JOURNAL_LEADERSHIP_MARKER", SSTR(2), myself(0).toString()));
  ASSERT_EQ(journal(1)->getLogSize(), 2);
}

TEST_F(Raft_Replicator, follower_has_larger_journal_than_leader) {
  // through the addition of several inconsistent entries, a follower
  // ended up with a larger journal than the leader

  ASSERT_TRUE(state(0)->observed(2, {}));
  ASSERT_TRUE(state(0)->becomeCandidate(2));
  ASSERT_TRUE(state(0)->ascend(2));

  ASSERT_TRUE(journal(1)->append(1, 0, make_req("supposed", "to", "be", "removed1")));
  ASSERT_TRUE(journal(1)->append(2, 0, make_req("supposed", "to", "be", "removed2")));
  ASSERT_TRUE(journal(1)->append(3, 0, make_req("supposed", "to", "be", "removed3")));

  ASSERT_EQ(state(1)->getCurrentTerm(), 0);

  // activate poller for #1
  poller(1);

  // launch!
  ASSERT_TRUE(replicator(0)->launch(myself(1), state(0)->getSnapshot()));

  // verify #1 recognized #0 as leader and that replication was successful
  RETRY_ASSERT_TRUE(journal(1)->getLogSize() == 2);

  RaftStateSnapshot snapshot = state(1)->getSnapshot();
  ASSERT_EQ(snapshot.term, 2);
  ASSERT_EQ(snapshot.leader, myself(0));
}

TEST_F(Raft_Dispatcher, validate_initial_state) {
  RaftInfo info = dispatcher()->info();
  ASSERT_EQ(info.clusterID, clusterID());
  ASSERT_EQ(info.myself, myself());
  ASSERT_EQ(info.term, 0);
  ASSERT_EQ(info.logSize, 1);
  ASSERT_TRUE(info.observers.empty());
  ASSERT_EQ(info.nodes, nodes());
  ASSERT_EQ(info.membershipEpoch, 0);

  RaftEntry entry;
  ASSERT_TRUE(dispatcher()->fetch(0, entry));
  ASSERT_EQ(entry.term, 0);
  ASSERT_EQ(entry.request, make_req("JOURNAL_UPDATE_MEMBERS", RaftMembers(nodes(), {}).toString(), info.clusterID));
}

TEST_F(Raft_Dispatcher, send_first_heartbeat) {
  // simulate heartbeat from #1 to #0
  RaftAppendEntriesRequest req;
  req.term = 1;
  req.leader = myself(1);
  req.prevIndex = 0;
  req.prevTerm = 0;
  req.commitIndex = 0;

  RaftAppendEntriesResponse resp = dispatcher()->appendEntries(std::move(req));
  ASSERT_EQ(resp.term, 1);
  ASSERT_TRUE(resp.outcome);
  ASSERT_EQ(resp.logSize, 1);
}

TEST_F(Raft_Dispatcher, throw_on_append_entries_from_myself) {
  RaftAppendEntriesRequest req;
  req.term = 2;
  req.leader = myself(0);
  req.prevIndex = 0;
  req.prevTerm = 0;
  req.commitIndex = 0;

  ASSERT_THROW(dispatcher()->appendEntries(std::move(req)), FatalException);
}

TEST_F(Raft_Dispatcher, add_entries) {
  RaftAppendEntriesRequest req;
  req.term = 2;
  req.leader = myself(1);
  req.prevIndex = 0;
  req.prevTerm = 0;
  req.commitIndex = 0;

  req.entries.push_back(RaftEntry {1, {"set", "qwerty", "123"}});
  req.entries.push_back(RaftEntry {1, {"hset", "abc", "123", "234"}});

  RaftAppendEntriesResponse resp = dispatcher()->appendEntries(std::move(req));
  ASSERT_EQ(resp.term, 2);
  ASSERT_TRUE(resp.outcome);
  ASSERT_EQ(resp.logSize, 3);

  // previous entry term mismatch, but verify term progressed
  req = RaftAppendEntriesRequest();
  req.term = 3;
  req.leader = myself(1);
  req.prevIndex = 2;
  req.prevTerm = 0;
  req.commitIndex = 0;

  resp = dispatcher()->appendEntries(std::move(req));
  ASSERT_EQ(resp.term, 3);
  ASSERT_FALSE(resp.outcome);
  ASSERT_EQ(resp.logSize, 3);

  // add three more entries with a different leader, while removing the last
  // entry as inconsistent

  req = RaftAppendEntriesRequest();
  req.term = 5;
  req.leader = myself(2);
  req.prevIndex = 1;
  req.prevTerm = 1;
  req.commitIndex = 1;

  req.entries.push_back(RaftEntry {3, {"sadd", "myset", "a"}});
  req.entries.push_back(RaftEntry {3, {"sadd", "myset", "b"}});
  req.entries.push_back(RaftEntry {3, {"sadd", "myset", "c"}});

  resp = dispatcher()->appendEntries(std::move(req));
  ASSERT_EQ(resp.term, 5);
  ASSERT_TRUE(resp.outcome) << resp.err;
  ASSERT_EQ(resp.logSize, 5);

  RaftEntry entry;
  ASSERT_TRUE(dispatcher()->fetch(2, entry));
  ASSERT_EQ(entry.term, 3);
  ASSERT_EQ(entry.request, make_req("sadd", "myset", "a"));

  // let's commit all entries
  req = RaftAppendEntriesRequest();
  req.term = 5;
  req.leader = myself(2);
  req.prevIndex = 4;
  req.prevTerm = 3;
  req.commitIndex = 4;

  resp = dispatcher()->appendEntries(std::move(req));
  ASSERT_EQ(resp.term, 5);
  ASSERT_TRUE(resp.outcome);
  ASSERT_EQ(resp.logSize, 5);

  // now let's say the new leader is a little confused, and tries to replicate the
  // last *committed* entry once again. Ensure the follower plays along
  req = RaftAppendEntriesRequest();
  req.term = 5;
  req.leader = myself(2);
  req.prevIndex = 3;
  req.prevTerm = 3;
  req.commitIndex = 4;

  req.entries.push_back(RaftEntry {3, {"sadd", "myset", "c"}});
  resp = dispatcher()->appendEntries(std::move(req));
  ASSERT_EQ(resp.term, 5);
  ASSERT_TRUE(resp.outcome);
  ASSERT_EQ(resp.logSize, 5);

  // the leader is still confused, and is sending an even older entry
  req = RaftAppendEntriesRequest();
  req.term = 5;
  req.leader = myself(2);
  req.prevIndex = 2;
  req.prevTerm = 3;
  req.commitIndex = 4;

  req.entries.push_back(RaftEntry {3, {"sadd", "myset", "b"}});
  resp = dispatcher()->appendEntries(std::move(req));
  ASSERT_EQ(resp.term, 5);
  ASSERT_TRUE(resp.outcome);
  ASSERT_EQ(resp.logSize, 5);

  // the leader is drunk and tries to overwrite the last committed entry with
  // a different one.
  req = RaftAppendEntriesRequest();
  req.term = 5;
  req.leader = myself(2);
  req.prevIndex = 3;
  req.prevTerm = 3;
  req.commitIndex = 4;

  req.entries.push_back(RaftEntry {3, {"sadd", "a different set", "c"}});
  ASSERT_THROW(dispatcher()->appendEntries(std::move(req)), FatalException);
}

TEST_F(Raft_Dispatcher, test_wrong_cluster_id) {
  // try to talk to a raft server while providing the wrong
  // cluster id, verify it sends us to hell

  poller(0);
  RaftTalker talker(myself(0), "random_cluster_id");

  RaftVoteRequest votereq;
  votereq.term = 1337;
  votereq.candidate = {"its_me_ur_leader", 1234};
  votereq.lastIndex = 35000000;
  votereq.lastTerm = 1000;

  ASSERT_REPLY(talker.requestVote(votereq).get(), "ERR not authorized to issue raft commands");

  std::vector<RedisRequest> reqs;
  std::vector<RaftTerm> terms;

  redisReplyPtr reply = talker.appendEntries(13737, myself(1), 3000, 100, 500, reqs, terms).get();
  ASSERT_REPLY(reply, "ERR not authorized to issue raft commands");
}

TEST_F(Raft_Voting, throws_with_requestvote_to_myself) {
  RaftVoteRequest req;
  req.term = 1;
  req.candidate = myself();
  req.lastTerm = 0;
  req.lastIndex = 2;

  ASSERT_THROW(dispatcher()->requestVote(req), FatalException);
}

TEST_F(Raft_Voting, no_double_voting_on_same_term) {
  RaftVoteRequest req;
  req.term = 1;
  req.candidate = myself(1);
  req.lastTerm = 0;
  req.lastIndex = 2;

  RaftVoteResponse resp = dispatcher()->requestVote(req);
  ASSERT_TRUE(resp.granted);

  req.candidate = myself(2);
  resp = dispatcher()->requestVote(req);
  ASSERT_FALSE(resp.granted);
}

TEST_F(Raft_Voting, no_votes_for_previous_terms) {
  RaftVoteRequest req;
  req.term = 1;
  req.candidate = myself(1);
  req.lastTerm = 0;
  req.lastIndex = 2;

  RaftVoteResponse resp = dispatcher()->requestVote(req);
  ASSERT_TRUE(resp.granted);

  req.term = 0;
  resp = dispatcher()->requestVote(req);
  ASSERT_FALSE(resp.granted);
}

TEST_F(Raft_Voting, no_votes_to_outdated_logs) {
  RaftVoteRequest req;
  req.term = 5;
  req.candidate =  myself(1);
  req.lastTerm = 0;
  req.lastIndex = 1;

  RaftVoteResponse resp = dispatcher()->requestVote(req);
  ASSERT_TRUE(resp.granted);

  // add a few requests to the log
  ASSERT_TRUE(journal()->append(1, 3, testreqs[0]));
  ASSERT_TRUE(journal()->append(2, 4, testreqs[1]));
  ASSERT_TRUE(journal()->append(3, 5, testreqs[2]));

  req.term = 6;
  req.candidate = myself(2);
  req.lastTerm = 4;
  req.lastIndex = 30;

  resp = dispatcher()->requestVote(req);
  ASSERT_FALSE(resp.granted);

  req.lastTerm = 5;
  req.lastIndex = 2;

  resp = dispatcher()->requestVote(req);
  ASSERT_FALSE(resp.granted);

  req.lastIndex = 4;
  resp = dispatcher()->requestVote(req);
  ASSERT_TRUE(resp.granted);
}

TEST(RaftTimeouts, basic_sanity) {
  RaftTimeouts timeouts(std::chrono::milliseconds(100),
    std::chrono::milliseconds(200),
    std::chrono::milliseconds(50));

  ASSERT_EQ(timeouts.getLow(), std::chrono::milliseconds(100));
  ASSERT_EQ(timeouts.getHigh(), std::chrono::milliseconds(200));
  ASSERT_EQ(timeouts.getHeartbeatInterval(), std::chrono::milliseconds(50));

  for(size_t i = 0; i < 10; i++) {
    std::chrono::milliseconds random = timeouts.getRandom();
    ASSERT_LE(random.count(), 200);
    ASSERT_GE(random.count(), 100);
  }
}

TEST_F(Raft_Election, basic_sanity) {
  ASSERT_TRUE(state()->observed(2, {}));

  // term mismatch, can't perform election
  RaftVoteRequest votereq;
  votereq.term = 1;
  ASSERT_FALSE(RaftElection::perform(votereq, *state()));

  // we have a leader already, can't do election
  ASSERT_TRUE(state()->observed(2, myself(1)));
  votereq.term = 2;
  ASSERT_FALSE(RaftElection::perform(votereq, *state()));

  // votereq.candidate must be empty
  votereq.candidate = myself(1);
  votereq.term = 3;
  ASSERT_TRUE(state()->observed(3, {}));
  ASSERT_THROW(RaftElection::perform(votereq, *state()), FatalException);
}

TEST_F(Raft_Election, leader_cannot_call_election) {
  ASSERT_TRUE(state()->observed(2, {}));
  ASSERT_TRUE(state()->becomeCandidate(2));
  ASSERT_TRUE(state()->ascend(2));

  RaftVoteRequest votereq;
  votereq.term = 2;
  ASSERT_FALSE(RaftElection::perform(votereq, *state()));
}

TEST_F(Raft_Election, observer_cannot_call_election) {
  // initialize node #0 not to be part of the cluster, thus an observer
  node(0, GlobalEnv::server(3));

  RaftStateSnapshot snapshot = state()->getSnapshot();
  ASSERT_EQ(snapshot.status, RaftStatus::FOLLOWER);

  ASSERT_TRUE(state()->observed(1, {}));

  RaftVoteRequest votereq;
  votereq.term = 1;

  ASSERT_FALSE(RaftElection::perform(votereq, *state()));
}

TEST_F(Raft_Election, complete_simple_election) {
  // initialize our raft cluster ..
  poller(0); poller(1); poller(2);

  ASSERT_TRUE(state(0)->observed(2, {}));
  ASSERT_TRUE(state(0)->becomeCandidate(2));

  RaftVoteRequest votereq;
  votereq.term = 2;
  votereq.lastIndex = 0;
  votereq.lastTerm = 0;

  ASSERT_TRUE(RaftElection::perform(votereq, *state(0)));

  RaftStateSnapshot snapshot0 = state(0)->getSnapshot();
  ASSERT_EQ(snapshot0.term, 2);
  ASSERT_EQ(snapshot0.leader, myself(0));
  ASSERT_EQ(snapshot0.status, RaftStatus::LEADER);

  // the rest of the nodes have not recognized the leadership yet, would need to
  // send heartbeats
}

TEST_F(Raft_Election, unsuccessful_election_not_enough_votes) {
  // #0 is alone in the cluster, its election rounds should always fail
  poller();

  ASSERT_TRUE(state()->observed(2, {}));
  ASSERT_TRUE(state()->becomeCandidate(2));

  RaftVoteRequest votereq;
  votereq.term = 2;
  votereq.lastIndex = 0;
  votereq.lastTerm = 0;

  ASSERT_FALSE(RaftElection::perform(votereq, *state(0)));
}

TEST_F(Raft_Election, split_votes_successful_election) {
  // let's have some more fun - have #1 already vote for term 2 for itself,
  // so it rejects any further requests
  // still possible to achieve quorum with #0 and #2
  poller(0); poller(1); poller(2);

  ASSERT_TRUE(state(0)->observed(2, {}));
  ASSERT_TRUE(state(0)->becomeCandidate(2));

  ASSERT_TRUE(state(1)->observed(2, {}));

  // #1 has already voted in term 2
  ASSERT_TRUE(state(1)->grantVote(2, myself(1)));

  RaftVoteRequest votereq;
  votereq.term = 2;
  votereq.lastIndex = 0;
  votereq.lastTerm = 0;

  ASSERT_TRUE(RaftElection::perform(votereq, *state(0)));

  RaftStateSnapshot snapshot0 = state(0)->getSnapshot();
  ASSERT_EQ(snapshot0.term, 2);
  ASSERT_EQ(snapshot0.leader, myself(0));
  ASSERT_EQ(snapshot0.status, RaftStatus::LEADER);
}

TEST_F(Raft_Election, split_votes_unsuccessful_election) {
  // this time both #1 and #2 have voted for themselves, should not be possible to
  // get a quorum
  poller(0); poller(1); poller(2);

  ASSERT_TRUE(state(0)->observed(2, {}));
  ASSERT_TRUE(state(0)->becomeCandidate(2));
  ASSERT_TRUE(state(1)->observed(2, {}));
  ASSERT_TRUE(state(2)->observed(2, {}));

  ASSERT_TRUE(state(1)->grantVote(2, myself(1)));
  ASSERT_TRUE(state(2)->grantVote(2, myself(2)));

  RaftVoteRequest votereq;
  votereq.term = 2;
  votereq.lastIndex = 0;
  votereq.lastTerm = 0;

  ASSERT_FALSE(RaftElection::perform(votereq, *state(0)));

  RaftStateSnapshot snapshot0 = state(0)->getSnapshot();
  ASSERT_EQ(snapshot0.term, 2);
  ASSERT_TRUE(snapshot0.leader.empty());
  ASSERT_EQ(snapshot0.status, RaftStatus::CANDIDATE);
}

TEST_F(Raft_Director, achieve_natural_election) {
  // spin up the directors and pollers - this fully simulates a 3-node cluster
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  std::vector<RaftStateSnapshot> snapshots;
  snapshots.push_back(state(0)->getSnapshot());
  snapshots.push_back(state(1)->getSnapshot());
  snapshots.push_back(state(2)->getSnapshot());

  // verify all have agreed on the same term
  ASSERT_EQ(snapshots[0].term, snapshots[1].term);
  ASSERT_EQ(snapshots[1].term, snapshots[2].term);

  // verify all have agreed on the same leader
  ASSERT_FALSE(snapshots[0].leader.empty());
  ASSERT_EQ(snapshots[0].leader, snapshots[1].leader);
  ASSERT_EQ(snapshots[1].leader, snapshots[2].leader);

  int leaderID = getServerID(snapshots[0].leader);
  ASSERT_GE(leaderID, 0);
  ASSERT_LE(leaderID, 2);

  ASSERT_EQ(snapshots[leaderID].status, RaftStatus::LEADER);
  for(int i = 0; i < 3; i++) {
    if(i != leaderID) {
      ASSERT_EQ(snapshots[i].status, RaftStatus::FOLLOWER) << i;
    }
  }

  // let's push a bunch of entries to the leader, and verify they get committed
  for(size_t i = 0; i < testreqs.size(); i++) {
    ASSERT_TRUE(journal(leaderID)->append(i+2, snapshots[0].term, testreqs[i]));
  }

  RETRY_ASSERT_TRUE(
    journal(0)->getCommitIndex() == (int64_t) testreqs.size()+1 &&
    journal(1)->getCommitIndex() == (int64_t) testreqs.size()+1 &&
    journal(2)->getCommitIndex() == (int64_t) testreqs.size()+1
  );

  // verify entries one by one, for all three journals
  for(size_t i = 0; i < testreqs.size(); i++) {
    for(size_t j = 0; j < 3; j++) {
      RaftEntry entry;

      ASSERT_TRUE(journal(j)->fetch(i+2, entry).ok());
      ASSERT_EQ(entry.request, testreqs[i]);
      ASSERT_EQ(entry.term, snapshots[0].term);
    }
  }
}

TEST_F(Raft_Director, late_arrival_in_established_cluster) {
  // spin up only two nodes
  spinup(0); spinup(1);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1));

  // verify they reached consensus
  std::vector<RaftStateSnapshot> snapshots;
  snapshots.push_back(state(0)->getSnapshot());
  snapshots.push_back(state(1)->getSnapshot());

  ASSERT_EQ(snapshots[0].term, snapshots[1].term);
  ASSERT_FALSE(snapshots[0].leader.empty());
  ASSERT_EQ(snapshots[0].leader, snapshots[1].leader);

  // spin up node #2, make sure it joins the cluster and doesn't disrupt the current leader
  spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  RaftStateSnapshot late_arrival = state(2)->getSnapshot();
  ASSERT_EQ(late_arrival.term, snapshots[0].term);
  ASSERT_EQ(late_arrival.leader, snapshots[1].leader);
}

TEST_F(Raft_Director, late_consensus) {
  // at first, node #0 is all alone and should not be able to ascend
  spinup(0);
  std::this_thread::sleep_for(raftclock()->getTimeouts().getHigh()*2);

  // verify the node tried to ascend, and failed
  RaftStateSnapshot snapshot = state(0)->getSnapshot();
  ASSERT_GE(snapshot.term, 1);
  ASSERT_TRUE(snapshot.leader.empty());
  ASSERT_TRUE( (snapshot.status == RaftStatus::FOLLOWER) || (snapshot.status == RaftStatus::CANDIDATE) );

  spinup(1);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1));

  // verify consensus reached
  std::vector<RaftStateSnapshot> snapshots;
  snapshots.push_back(state(0)->getSnapshot());
  snapshots.push_back(state(1)->getSnapshot());

  ASSERT_EQ(snapshots[0].term, snapshots[1].term);
  ASSERT_FALSE(snapshots[0].leader.empty());
  ASSERT_EQ(snapshots[0].leader, snapshots[1].leader);

  // spin up node #2, ensure it doesn't disrupt current leader
  spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  RaftStateSnapshot late_arrival = state(2)->getSnapshot();
  ASSERT_EQ(late_arrival.term, snapshots[0].term);
  ASSERT_EQ(late_arrival.leader, snapshots[0].leader);
  ASSERT_EQ(late_arrival.status, RaftStatus::FOLLOWER);
}

TEST_F(Raft_Director, election_with_different_journals) {
  // start an election between #0 and #1 where #1 is guaranteed to win due
  // to more up-to-date journal

  RedisRequest req = {"set", "asdf", "abc"};
  ASSERT_TRUE(journal(1)->append(1, 0, req));

  spinup(0); spinup(1);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1));

  RaftStateSnapshot snapshot = state(0)->getSnapshot();
  ASSERT_EQ(snapshot.leader, myself(1));
  ASSERT_EQ(snapshot.status, RaftStatus::FOLLOWER);

  snapshot = state(1)->getSnapshot();
  ASSERT_EQ(snapshot.leader, myself(1));
  ASSERT_EQ(snapshot.status, RaftStatus::LEADER);
}

TEST_F(Raft_CommitTracker, basic_sanity) {
  ASSERT_THROW(RaftCommitTracker(*journal(0), 1), FatalException);
  ASSERT_THROW(RaftCommitTracker(*journal(0), 0), FatalException);

  RaftCommitTracker tracker(*journal(0), 2);
  ASSERT_EQ(journal(0)->getCommitIndex(), 0);

  // populate #0's journal
  for(size_t i = 0; i < testreqs.size(); i++) {
    ASSERT_TRUE(journal(0)->append(i+1, 0, testreqs[i]));
  }

  RaftMatchIndexTracker emptyTracker;
  emptyTracker.update(300);
  ASSERT_EQ(journal(0)->getCommitIndex(), 0);

  RaftMatchIndexTracker matchIndex1(tracker, myself(1));
  RaftMatchIndexTracker matchIndex2(tracker, myself(2));

  matchIndex1.update(1);
  ASSERT_EQ(journal(0)->getCommitIndex(), 1);
  ASSERT_THROW(matchIndex1.update(0), FatalException);

  matchIndex2.update(1);
  ASSERT_EQ(journal(0)->getCommitIndex(), 1);

  matchIndex2.update(2);
  ASSERT_EQ(journal(0)->getCommitIndex(), 2);

  matchIndex1.update(3);
  ASSERT_EQ(journal(0)->getCommitIndex(), 3);

  tracker.updateQuorum(3);
  matchIndex1.update(4);
  ASSERT_EQ(journal(0)->getCommitIndex(), 3);

  matchIndex2.update(4);
  ASSERT_EQ(journal(0)->getCommitIndex(), 4);

  matchIndex1.update(10);
  ASSERT_EQ(journal(0)->getCommitIndex(), 4);

  RaftMatchIndexTracker matchIndex3(tracker, RaftServer("some_server", 1234));
  matchIndex3.update(15); // now we have 10, 4, 15
  ASSERT_EQ(journal(0)->getCommitIndex(), 10);

  matchIndex2.update(11); // now we have 10, 11, 15
  ASSERT_EQ(journal(0)->getCommitIndex(), 11);

  matchIndex1.update(16); // now we have 16, 11, 15
  ASSERT_EQ(journal(0)->getCommitIndex(), 15);
}

TEST(RaftMembers, basic_sanity) {
  std::vector<RaftServer> nodes = {
    {"server1", 245},
    {"localhost", 789},
    {"server2.cern.ch", 1789}
  };

  std::vector<RaftServer> observers = {
    {"observer1", 1234},
    {"observer2", 789},
    {"observer3.cern.ch", 111}
  };

  RaftMembers members(nodes, observers);
  ASSERT_EQ(members.nodes, nodes);
  ASSERT_EQ(members.observers, observers);

  RaftMembers members2(members.toString());
  ASSERT_EQ(members, members2);
  ASSERT_EQ(members.nodes, members2.nodes);
  ASSERT_EQ(members.observers, members2.observers);
  ASSERT_EQ(members.toString(), members2.toString());
}

TEST(RaftMembers, no_observers) {
  std::vector<RaftServer> nodes = {
    {"server1", 245},
    {"localhost", 789},
    {"server2.cern.ch", 1789}
  };

  std::vector<RaftServer> observers;
  RaftMembers members(nodes, observers);
  ASSERT_EQ(members.nodes, nodes);
  ASSERT_EQ(members.observers, observers);

  RaftMembers members2(members.toString());
  ASSERT_EQ(members, members2);
  ASSERT_EQ(members.nodes, members2.nodes);
  ASSERT_EQ(members.observers, members2.observers);
  ASSERT_EQ(members.toString(), members2.toString());
}
