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
#include "qclient/GlobalInterceptor.hh"
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
    Status st;
    ret = ShardDirectory::create(path, clusterID, "default", nodes, 0, FsyncPolicy::kAsync, {}, st);
    st.assertOk();
    shardDirCache[path] = ret;
  }

  ret->obliterate(clusterID, nodes, 0, FsyncPolicy::kAsync, {} );
  return ret;
}

RaftServer GlobalEnv::server(int id) {
  RaftServer srv;
  srv.hostname = SSTR("server" << id);
  srv.port = 23456 + id;

  qclient::GlobalInterceptor::addIntercept(
    qclient::Endpoint(srv.hostname, srv.port),
    qclient::Endpoint("127.0.0.1", srv.port)
  );

  return srv;
}

::testing::Environment* const commonStatePtr = ::testing::AddGlobalTestEnvironment(new GlobalEnv);
GlobalEnv &commonState(*(GlobalEnv*)commonStatePtr);

TestCluster::TestCluster(RaftTimeouts timeouts, RaftClusterID clust,
    const std::vector<RaftServer> &nd, int initialActiveNodes)
: clusterid(clust), clusterTimeouts(timeouts), allNodes(nd) {

  if(initialActiveNodes < 0) {
    initialNodes = allNodes;
  }

  for(int i = 0; i < initialActiveNodes; i++) {
    initialNodes.emplace_back(allNodes[i]);
  }

  Connection::setPhantomBatchLimit(100);
}

TestCluster::TestCluster(RaftClusterID clust, const std::vector<RaftServer> &nd,
  int initialActiveNodes)
: TestCluster(testconfig.raftTimeouts, clust, nd, initialActiveNodes) {}

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

AsioPoller* TestCluster::poller(int id) {
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

qclient::Members TestCluster::members(int id) {
  return node(id)->members();
}

qclient::QClient* TestCluster::tunnel(int id) {
  return node(id)->tunnel();
}

std::unique_ptr<qclient::Handshake> TestCluster::makeQClientHandshake(int id) {
  return node(id)->makeQClientHandshake();
}

qclient::Options TestCluster::makeNoRedirectOptions(int id) {
  return node(id)->makeNoRedirectOptions();
}

RaftHeartbeatTracker* TestCluster::heartbeatTracker(int id) {
  return node(id)->group()->heartbeatTracker();
}

const RaftContactDetails* TestCluster::contactDetails(int id) {
  return node(id)->group()->contactDetails();
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

Publisher* TestCluster::publisher(int id) {
  return node(id)->group()->publisher();
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
  if(newserver.empty()) newserver = allNodes[id];
  TestNode *ret = new TestNode(newserver, clusterID(), timeouts(), initialNodes);
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
  for(size_t i = 0; i < allNodes.size(); i++) {
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

qclient::SubscriptionOptions TestCluster::reasonableSubscriptionOptions() {
  qclient::SubscriptionOptions opts;
  opts.handshake = makeQClientHandshake();
  return opts;
}

RaftTimeouts TestCluster::timeouts() {
  return clusterTimeouts;
}

TestNode::TestNode(RaftServer me, RaftClusterID clust, RaftTimeouts timeouts, const std::vector<RaftServer> &nd)
: myselfSrv(me), clusterID(clust), initialNodes(nd) {

  std::string shardPath = SSTR(commonState.testdir << "/" << myself().hostname << "-" << myself().port);
  Configuration config;

  bool status = Configuration::fromString(SSTR(
    "redis.mode raft\n" <<
    "redis.database " << shardPath << "\n"
    "redis.myself " << myselfSrv.toString() << "\n"
    "redis.password 1234567890-qwerty-0987654321-ytrewq\n"
  ), config);

  if(!status) {
    qdb_throw("error reading configuration");
  }

  shardPath += "/";

  // We inject the shard directory in QDB node.
  sharddirptr = commonState.getShardDirectory(shardPath, clusterID, initialNodes);
  qdbnodeptr = new QuarkDBNode(config, timeouts, sharddirptr);
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

qclient::Members TestNode::members() {
  qclient::Members memb;
  std::vector<RaftServer> clusterNodes = this->nodes();

  for(auto it = clusterNodes.begin(); it != clusterNodes.end(); it++) {
    memb.push_back(it->hostname, it->port);
  }

  return memb;
}

AsioPoller* TestNode::poller() {
  if(pollerptr == nullptr) {
    pollerptr = new AsioPoller(myself().port, 5, quarkdbNode());
  }
  return pollerptr;
}

qclient::Options TestNode::makeNoRedirectOptions() {
  qclient::Options options;
  options.transparentRedirects = false;
  options.handshake = makeQClientHandshake();

  return options;
}

std::unique_ptr<qclient::Handshake> TestNode::makeQClientHandshake() {
  if(group()->contactDetails()->getPassword().empty()) {
    return {};
  }

  return std::unique_ptr<qclient::Handshake>(new qclient::HmacAuthHandshake(group()->contactDetails()->getPassword()));
}

qclient::QClient* TestNode::tunnel() {
  if(tunnelptr == nullptr) {
    tunnelptr = new qclient::QClient(myself().hostname, myself().port, makeNoRedirectOptions());
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

bool IptablesHelper::singleDropPackets(int port) {
  return system(SSTR("iptables -I OUTPUT -p tcp --dest 127.0.0.1 --dport " << port << " -j DROP").c_str()) == 0;
}

bool IptablesHelper::singleAcceptPackets(int port) {
  return system(SSTR("iptables -I OUTPUT -p tcp --dest 127.0.0.1 --dport " << port << " -j ACCEPT").c_str()) == 0;
}


}
