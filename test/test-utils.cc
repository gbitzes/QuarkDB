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

#include "ShardDirectory.hh"
#include "config/test-config.hh"
#include "test-utils.hh"
#include "Utils.hh"
#include <vector>
#include <string>
#include "Shard.hh"
#include "raft/RaftGroup.hh"
#include "raft/RaftJournal.hh"
#include "QuarkDBNode.hh"
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

void GlobalEnv::clearConnectionCache() {
  qdb_info("Global environment: clearing connection cache.");

  for(auto& kv : shardDirCache) {
    delete kv.second;
  }
  shardDirCache.clear();

  if(!testdir.empty()) {
    system(SSTR("rm -r " << testdir).c_str());
    system(SSTR("mkdir " << testdir).c_str());
  }
}

void GlobalEnv::SetUp() {
  clearConnectionCache();
}

void GlobalEnv::TearDown() {
  clearConnectionCache();
}

ShardDirectory* GlobalEnv::getShardDirectory(const std::string &path, RaftClusterID clusterID, const std::vector<RaftServer> &nodes) {
  ShardDirectory *ret = shardDirCache[path];
  if(ret == nullptr) {
    ret = ShardDirectory::create(path, clusterID, "default", nodes);
    shardDirCache[path] = ret;
  }

  ret->obliterate(clusterID, nodes);
  return ret;
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

  if(!testconfig.databaseReuse) {
    commonState.clearConnectionCache();
  }
}

RaftClusterID TestCluster::clusterID() {
  return clusterid;
}

ShardDirectory* TestCluster::shardDirectory(int id) {
  return node(id)->shardDirectory();
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

RaftCommitTracker* TestCluster::commitTracker(int id) {
  return node(id)->group()->commitTracker();
}

RaftConfig* TestCluster::raftconfig(int id) {
  return node(id)->group()->config();
}

RaftTrimmer* TestCluster::trimmer(int id) {
  return node(id)->group()->trimmer();
}

void TestCluster::killTunnel(int id) {
  return node(id)->killTunnel();
}

TestNode* TestCluster::node(int id, const RaftServer &srv) {
  auto it = testnodes.find(id);
  if(it != testnodes.end()) {
    return it->second;
  }

  RaftServer newserver = srv;
  if(newserver.empty()) newserver = initialNodes[id];
  TestNode *ret = new TestNode(newserver, clusterID(), initialNodes);
  testnodes[id] = ret;
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
      ret.push_back(state(i)->getSnapshot()->leader);
    }
  }
  return ret;
}

int TestCluster::getLeaderID() {
  return getServerID(state(0)->getSnapshot()->leader);
}

TestNode::TestNode(RaftServer me, RaftClusterID clust, const std::vector<RaftServer> &nd)
: myselfSrv(me), clusterID(clust), initialNodes(nd) {

  std::string shardPath = SSTR(commonState.testdir << "/" << myself().hostname << "-" << myself().port);
  Configuration config;

  qdb_warn(shardPath);

  bool status = Configuration::fromString(SSTR(
    "redis.mode raft\n" <<
    "redis.database " << shardPath << "\n"
    "redis.myself " << myselfSrv.toString() << "\n"
  ), config);

  if(!status) {
    qdb_warn("Error reading configuration, crashing");
    std::quick_exit(1);
  }

  shardPath += "/";

  // We inject the shard directory in QDB node.
  sharddirptr = commonState.getShardDirectory(shardPath, clusterID, initialNodes);
  qdbnodeptr = new QuarkDBNode(config, testconfig.raftTimeouts, sharddirptr);
}

ShardDirectory* TestNode::shardDirectory() {
  return sharddirptr;
}

Shard* TestNode::shard() {
  return quarkdbNode()->getShard();
}

RaftGroup* TestNode::group() {
  return shard()->getRaftGroup();
}

QuarkDBNode* TestNode::quarkdbNode() {
  return qdbnodeptr;
}

TestNode::~TestNode() {
  if(pollerptr) delete pollerptr;
  if(tunnelptr) delete tunnelptr;
  if(qdbnodeptr) delete qdbnodeptr;
}

RaftServer TestNode::myself() {
  return myselfSrv;
}

std::vector<RaftServer> TestNode::nodes() {
  return group()->journal()->getNodes();
}

Poller* TestNode::poller() {
  if(pollerptr == nullptr) {
    pollerptr = new Poller(myself().port, shard());
  }
  return pollerptr;
}

qclient::QClient* TestNode::tunnel() {
  if(tunnelptr == nullptr) {
    tunnelptr = new qclient::QClient(myself().hostname, myself().port);
  }
  return tunnelptr;
}

void TestNode::killTunnel() {
  if(tunnelptr != nullptr) {
    delete tunnelptr;
    tunnelptr = nullptr;
  }
}

void TestNode::spinup() {
  shard()->spinup();
  poller();
}

void TestNode::spindown() {
  if(pollerptr) {
    delete pollerptr;
    pollerptr = nullptr;
  }
  shard()->spindown();
}

}
