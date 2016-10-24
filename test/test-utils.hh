// ----------------------------------------------------------------------
// File: test-utils.hh
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

#ifndef __QUARKDB_TEST_UTILS_H__
#define __QUARKDB_TEST_UTILS_H__

#include <sys/socket.h>
#include <sys/un.h>
#include <vector>
#include "Common.hh"
#include "raft/RaftDispatcher.hh"
#include "raft/RaftReplicator.hh"
#include "raft/RaftDirector.hh"
#include "Poller.hh"
#include <gtest/gtest.h>

namespace quarkdb {

extern std::vector<RedisRequest> testreqs;

// necessary because C macros are dumb and don't undestand
// universal initialization with brackets {}
template<typename... Args>
RedisRequest make_req(Args... args) {
  return RedisRequest { args... };
}

class GlobalEnv : public testing::Environment {
public:
  virtual void SetUp() override;
  virtual void TearDown() override;

  // initialize a clean rocksdb database or journal. The connection is cached,
  // because even if rocksdb is local, it takes a long time to open one.
  // (often 50+ ms)
  RocksDB *getRocksDB(const std::string &path);
  RaftJournal *getJournal(const std::string &path, RaftClusterID clusterID, const std::vector<RaftServer> &nodes);
  const std::string testdir = "/tmp/quarkdb-tests";
  static RaftServer server(int id);
private:
  std::map<std::string, RocksDB*> rocksdbCache;
  std::map<std::string, RaftJournal*> journalCache;
};
extern GlobalEnv &commonState;

// Includes everything needed to simulate a single raft-enabled server.
// Everything is initialized lazily, so if you only want to test the journal for example,
// this is possible, too. Just don't call eg raft(), and you won't have to worry
// about raft messing up your variables and terms due to timeouts.
class TestNode {
public:
  TestNode(RaftServer myself, RaftClusterID clusterID, const std::vector<RaftServer> &nodes);
  ~TestNode();

  RocksDB *rocksdb();
  RaftJournal *journal();
  RaftDispatcher *dispatcher();
  RaftState *state();
  RaftReplicator *replicator();
  Poller *poller();
  RaftClock *raftClock();
  RaftDirector *director();
  Tunnel *tunnel();

  RaftServer myself();
  std::vector<RaftServer> nodes();
  std::string unixsocket();
private:
  RaftServer myselfSrv;
  RaftClusterID clusterID;
  std::vector<RaftServer> initialNodes;

  RocksDB *rocksdbptr = nullptr;
  RaftJournal *journalptr = nullptr;
  RaftDispatcher *raftdispatcherptr = nullptr;
  RaftReplicator *replicatorptr = nullptr;
  Poller *pollerptr = nullptr;
  RaftState *raftstateptr = nullptr;
  RaftClock *raftclockptr = nullptr;
  RaftDirector *raftdirectorptr = nullptr;
  Tunnel *tunnelptr = nullptr;

  std::string unixsocketpath;
};

// Contains everything needed to simulate a cluster with an arbitrary number of nodes.
// Everything is initialized lazily, including the nodes of the cluster themselves.
class TestCluster : public ::testing::Test {
public:
  TestCluster(RaftClusterID clusterID, const std::vector<RaftServer> &nodes);
  ~TestCluster();

  RocksDB* rocksdb(int id = 0);
  RaftJournal* journal(int id = 0);
  RaftDispatcher *dispatcher(int id = 0);
  RaftState *state(int id = 0);
  RaftReplicator *replicator(int id = 0);
  Poller *poller(int id = 0);
  RaftServer myself(int id = 0);
  RaftDirector *director(int id = 0);
  Tunnel *tunnel(int id = 0);

  // In some tests, the latency of opening rocksdb can kill us, since by the
  // time the db is open raft starts timing out.
  // This function will prepare a node, so that spinning it up later is instant.
  void prepare(int id);

  // spin up a node,
  void spinup(int id);

  // initialize nodes using information passed on the nodes variable, except if srv is set
  TestNode* node(int id = 0, const RaftServer &srv = {});
  std::vector<RaftServer> nodes(int id = 0);
  std::string unixsocket(int id = 0);
  RaftClusterID clusterID();

  int getServerID(const RaftServer &srv);
private:
  std::string rocksdbPath(int id = 0);

  RaftClusterID clusterid;
  std::vector<RaftServer> initialNodes;

  std::map<int, TestNode*> testnodes;
};

// Convenience class. Want to run tests on a simulated cluster of 3 nodes?
// Inherit your test fixture from here.
class TestCluster3Nodes : public TestCluster {
public:
  TestCluster3Nodes() : TestCluster("a9b9e979-5428-42e9-8a52-f675c39fdf80", {
    GlobalEnv::server(0),
    GlobalEnv::server(1),
    GlobalEnv::server(2)
  }) {
    Tunnel::clearIntercepts();
  };
};

class UnixSocketListener {
private:
  struct sockaddr_un local, remote;
  unsigned int s;
  size_t len;
  socklen_t t;
public:
  UnixSocketListener(const std::string path) {
    s = socket(AF_UNIX, SOCK_STREAM, 0);
    local.sun_family = AF_UNIX;
    strcpy(local.sun_path, path.c_str());
    len = strlen(local.sun_path) + sizeof(local.sun_family);
    bind(s, (struct sockaddr *)&local, len);
    listen(s, 1);
    t = sizeof(remote);
  }

  ~UnixSocketListener() {

  }

  int accept() {
    return ::accept(s, (struct sockaddr *)&remote, &t);
  }
};



}

#endif
