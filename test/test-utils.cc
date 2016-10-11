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
#include "Tunnel.hh"
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
  {"sadd", "myset", "d"}
};

void GlobalEnv::TearDown() {
  for(auto& kv : rocksdbCache) {
    delete kv.second;
  }

  for(auto& kv : journalCache) {
    delete kv.second;
  }
}

RocksDB* GlobalEnv::getRocksDB(const std::string &path) {
  RocksDB *ret = rocksdbCache[path];
  if(ret == nullptr) {
    ret = new RocksDB(path);
    rocksdbCache[path] = ret;
  }

  ret->flushall();
  return ret;
}

RaftJournal* GlobalEnv::getJournal(const std::string &path, RaftClusterID clusterID, const std::vector<RaftServer> &nodes) {
  RaftJournal *ret = journalCache[path];
  if(ret == nullptr) {
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
  srv.port = 7776 + id;
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

RocksDB* TestCluster::rocksdb(int id) {
  return node(id)->rocksdb();
}

RaftJournal* TestCluster::journal(int id) {
  return node(id)->journal();
}

Raft* TestCluster::raft(int id) {
  return node(id)->raft();
}

RaftState* TestCluster::state(int id) {
  return node(id)->state();
}

RaftReplicator* TestCluster::replicator(int id) {
  return node(id)->replicator();
}

Poller* TestCluster::poller(int id) {
  return node(id)->poller();
}

RaftServer TestCluster::myself(int id) {
  return node(id)->myself();
}

std::vector<RaftServer> TestCluster::nodes(int id) {
  return node(id)->nodes();
}

std::string TestCluster::unixsocket(int id) {
  return node(id)->unixsocket();
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

TestNode::TestNode(RaftServer me, RaftClusterID clust, const std::vector<RaftServer> &nd)
: myselfSrv(me), clusterID(clust), initialNodes(nd) {

}

TestNode::~TestNode() {
  if(pollerptr) delete pollerptr;
  if(raftptr) delete raftptr;
  if(replicatorptr) delete replicatorptr;
  if(raftstateptr) delete raftstateptr;
}

RocksDB* TestNode::rocksdb() {
  // must be cached locally, because with every call to getRocksDB()
  // the contents are reset
  // NOT deleted by ~TestNode - ownership is retained by commonState for caching.
  if(rocksdbptr == nullptr) {
    rocksdbptr = commonState.getRocksDB(SSTR(commonState.testdir << "/rocksdb-" << myself().hostname << "-" << myself().port));
  }
  return rocksdbptr;
}

RaftJournal* TestNode::journal() {
  // see TestNode::rocksdb()
  if(journalptr == nullptr) {
    journalptr = commonState.getJournal(SSTR(commonState.testdir << "/journal-" << myself().hostname << "-" << myself().port), clusterID, initialNodes);
  }
  return journalptr;
}

RaftServer TestNode::myself() {
  return myselfSrv;
}

std::vector<RaftServer> TestNode::nodes() {
  return journal()->getNodes();
}

std::string TestNode::unixsocket() {
  if(unixsocketpath.empty()) {
    unixsocketpath = SSTR(commonState.testdir << "/socket-" << myself().hostname << "-" << myself().port);
    unlink(unixsocketpath.c_str());
    Tunnel::addIntercept(myself().hostname, myself().port, unixsocketpath);
  }
  return unixsocketpath;
}

Poller* TestNode::poller() {
  if(pollerptr == nullptr) {
    pollerptr = new Poller(unixsocket(), raft());
  }
  return pollerptr;
}

Raft* TestNode::raft() {
  if(raftptr == nullptr) {
    raftptr = new Raft(*journal(), *rocksdb(), *state());
  }
  return raftptr;
}

RaftState* TestNode::state() {
  if(raftstateptr == nullptr) {
    raftstateptr = new RaftState(*journal(), myself());
  }
  return raftstateptr;
}

RaftReplicator* TestNode::replicator() {
  if(replicatorptr == nullptr) {
    replicatorptr = new RaftReplicator(*journal(), *state(), defaultTimeouts);
  }
  return replicatorptr;
}

}
