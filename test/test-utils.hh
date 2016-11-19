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

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

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
// this is possible, too. Just don't call eg director(), and you won't have to worry
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
  RaftClusterID clusterID();

  int getServerID(const RaftServer &srv);
  std::vector<RaftServer> retrieveLeaders();
  int getLeaderID();
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
  }) { };
};

class TestCluster5Nodes : public TestCluster {
public:
  TestCluster5Nodes() : TestCluster("a9b9e979-5428-42e9-8a52-f675c39fdf80", {
    GlobalEnv::server(0),
    GlobalEnv::server(1),
    GlobalEnv::server(2),
    GlobalEnv::server(3),
    GlobalEnv::server(4)
  }) { };
};

class SocketListener {
private:
  int s;
  struct sockaddr_in remote;
public:
  SocketListener(int port) {
    struct addrinfo hints, *servinfo, *p;
    int rv, yes = 1;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    if ((rv = getaddrinfo(NULL, std::to_string(port).c_str(), &hints, &servinfo)) != 0) {
      fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
      exit(1);
    }

    // loop through all the results and bind to the first we can
    for(p = servinfo; p != NULL; p = p->ai_next) {
      if ((s = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
        perror("server: socket");
        continue;
      }
      if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &yes,sizeof(int)) == -1) {
        perror("setsockopt");
        exit(1);
      }
      if (bind(s, p->ai_addr, p->ai_addrlen) == -1) {
        close(s);
        perror("server: bind");
        continue;
      }
      break;
    }

    freeaddrinfo(servinfo); // all done with this structure

    if (p == NULL) {
      fprintf(stderr, "server: failed to bind\n");
      exit(1);
    }

    if (listen(s, 10) == -1) {
      perror("listen");
      exit(1);
    }
  }
  ~SocketListener() {
    ::shutdown(s, SHUT_RDWR);
    close(s);
  }

  int accept() {
    socklen_t remoteSize = sizeof(remote);
    return ::accept(s, (struct sockaddr *)&remote, &remoteSize);
  }
};



}

#endif
