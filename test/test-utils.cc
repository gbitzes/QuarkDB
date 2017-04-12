// ----------------------------------------------------------------------
// File: test-utils.cc
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

#include "test-utils.hh"
#include "Utils.hh"
#include <vector>
#include <string>
#include <gtest/gtest.h>

namespace quarkdb {

std::vector<RedisRequest> testreqs = {
  {"set", "abc", "123"},
  {"set", "123", "abc"},
  {"hset", "myhash", "value", "234" },
  {"sadd", "myset", "a"},
  {"sadd", "myset", "b"},
  {"sadd", "myset", "c"},
  {"sadd", "myset", "d"},
  {"hset", "myhash", "key1", "val1"},
  {"hset", "myhash", "key2", "val2"},
  {"hset", "myhash", "key3", "val3"},
  {"hset", "myhash", "key4", "val4"},
  {"hset", "myhash", "key5", "val5"},
  {"hset", "myhash", "key6", "val6"},
  {"hset", "myhash", "key7", "val7"},
  {"hset", "myhash", "key8", "val8"},
  {"hset", "myhash", "key9", "val9"}
};

void GlobalEnv::TearDown() {
  for(auto& kv : smCache) {
    delete kv.second;
  }
  smCache.clear();

  for(auto& kv : journalCache) {
    delete kv.second;
  }
  journalCache.clear();
}

StateMachine* GlobalEnv::getStateMachine(const std::string &path) {
  StateMachine *ret = smCache[path];
  if(ret == nullptr) {
    mkpath_or_die(path, 0755);
    ret = new StateMachine(path);
    smCache[path] = ret;
  }

  ret->reset();
  return ret;
}

RaftJournal* GlobalEnv::getJournal(const std::string &path, RaftClusterID clusterID, const std::vector<RaftServer> &nodes) {
  RaftJournal *ret = journalCache[path];
  if(ret == nullptr) {
    mkpath_or_die(path, 0755);
    ret = new RaftJournal(path, clusterID, nodes);
    journalCache[path] = ret;
  }

  ret->obliterate(clusterID, nodes);
  return ret;
}

void GlobalEnv::SetUp() {
  if(!testdir.empty()) {
    system(SSTR("rm -r " << testdir).c_str());
    system(SSTR("mkdir " << testdir).c_str());
  }
}

RaftServer GlobalEnv::server(int id) {
  RaftServer srv;
  srv.hostname = SSTR("server" << id);
  srv.port = 23456 + id;

  qclient::QClient::addIntercept(srv.hostname, srv.port, "127.0.0.1", srv.port);
  return srv;
}

::testing::Environment* const commonStatePtr = ::testing::AddGlobalTestEnvironment(new GlobalEnv);
GlobalEnv &commonState(*(GlobalEnv*)commonStatePtr);

TestCluster::TestCluster(RaftClusterID clust, const std::vector<RaftServer> &nd)
: clusterid(clust), initialNodes(nd) {
}

TestCluster::~TestCluster() {
  for(auto &kv : testnodes) {
    delete kv.second;
  }
}

RaftClusterID TestCluster::clusterID() {
  return clusterid;
}

StateMachine* TestCluster::stateMachine(int id) {
  return node(id)->group()->stateMachine();
}

RaftJournal* TestCluster::journal(int id) {
  return node(id)->group()->journal();
}

RaftDispatcher* TestCluster::dispatcher(int id) {
  return node(id)->group()->dispatcher();
}

RaftState* TestCluster::state(int id) {
  return node(id)->group()->state();
}

RaftReplicator* TestCluster::replicator(int id) {
  return node(id)->group()->replicator();
}

Poller* TestCluster::poller(int id) {
  return node(id)->poller();
}

RaftDirector* TestCluster::director(int id) {
  return node(id)->group()->director();
}

RaftServer TestCluster::myself(int id) {
  return node(id)->group()->myself();
}

std::vector<RaftServer> TestCluster::nodes(int id) {
  return node(id)->nodes();
}

qclient::QClient* TestCluster::tunnel(int id) {
  return node(id)->tunnel();
}

RaftClock* TestCluster::raftclock(int id) {
  return node(id)->group()->raftclock();
}

RaftLease* TestCluster::lease(int id) {
  return node(id)->group()->lease();
}

TestNode* TestCluster::node(int id, const RaftServer &srv) {
  TestNode *ret = testnodes[id];
  if(ret == nullptr) {
    RaftServer newserver = srv;
    if(newserver.empty()) newserver = initialNodes[id];
    ret = new TestNode(newserver, clusterID(), initialNodes);
    testnodes[id] = ret;
  }
  return ret;
}

void TestCluster::spinup(int id) {
  qdb_info("Spinning up node #" << id)
  node(id)->spinup();
}

void TestCluster::spindown(int id) {
  qdb_info("Spinning down node  #" << id)
  node(id)->spindown();
}

void TestCluster::prepare(int id) {
  qdb_info("Preparing node #" << id);
  journal(id);
  stateMachine(id);
}

int TestCluster::getServerID(const RaftServer &srv) {
  for(size_t i = 0; i < initialNodes.size(); i++) {
    if(myself(i) == srv) return i;
  }
  return -1;
}

std::vector<RaftServer> TestCluster::retrieveLeaders() {
  std::vector<RaftServer> ret;
  for(size_t i = 0; i < initialNodes.size(); i++) {
    if(testnodes.count(i) > 0) {
      ret.push_back(state(i)->getSnapshot().leader);
    }
  }
  return ret;
}

int TestCluster::getLeaderID() {
  return getServerID(state(0)->getSnapshot().leader);
}

TestNode::TestNode(RaftServer me, RaftClusterID clust, const std::vector<RaftServer> &nd)
: myselfSrv(me), clusterID(clust), initialNodes(nd) {

  StateMachine *smPtr = commonState.getStateMachine(SSTR(commonState.testdir << "/" << myself().hostname << "-" << myself().port << "/state-machine"));
  RaftJournal *journalPtr = commonState.getJournal(SSTR(commonState.testdir << "/" << myself().hostname << "-" << myself().port << "/raft-journal"), clusterID, initialNodes);
  raftgroup = new RaftGroup(*journalPtr, *smPtr, myself(), aggressiveTimeouts);
}

RaftGroup* TestNode::group() {
  return raftgroup;
}

TestNode::~TestNode() {
  if(pollerptr) delete pollerptr;
  if(tunnelptr) delete tunnelptr;
  if(raftgroup) delete raftgroup;
}

RaftServer TestNode::myself() {
  return myselfSrv;
}

std::vector<RaftServer> TestNode::nodes() {
  return group()->journal()->getNodes();
}

Poller* TestNode::poller() {
  if(pollerptr == nullptr) {
    pollerptr = new Poller(myself().port, group()->dispatcher());
  }
  return pollerptr;
}

qclient::QClient* TestNode::tunnel() {
  if(tunnelptr == nullptr) {
    tunnelptr = new qclient::QClient(myself().hostname, myself().port);
  }
  return tunnelptr;
}

void TestNode::spinup() {
  poller();
  group()->spinup();
}

void TestNode::spindown() {
  if(pollerptr) {
    delete pollerptr;
    pollerptr = nullptr;
  }
  group()->spindown();
}

}
