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

namespace quarkdb {

extern std::vector<RedisRequest> testreqs;
extern std::vector<RaftServer> testnodes;
extern std::vector<RaftServer> testnodes3;
extern std::vector<RaftClusterID> test_clusterIDs;
extern std::vector<std::string> test_unixsockets;
extern std::vector<std::string> test_journals;
extern std::vector<std::string> test_statemachines;

// necessary because C macros are dumb and don't undestand
// universal initialization with brackets {}
template<typename... Args>
RedisRequest make_req(Args... args) {
  return RedisRequest { args... };
}

class TestsCommonState {
public:
  TestsCommonState();
};
extern TestsCommonState commonState;

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
