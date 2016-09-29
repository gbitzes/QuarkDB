// ----------------------------------------------------------------------
// File: Common.hh
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

#ifndef __QUARKDB_COMMON_H__
#define __QUARKDB_COMMON_H__

namespace quarkdb {

enum class TraceLevel {
  off = 0,
  error = 1,
  warning = 2,
  info = 3,
  debug = 4
};

class Status {
public:
  // up to kTryAgain we are compatible to rocksdb error codes
  enum Code {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5,
    kMergeInProgress = 6,
    kIncomplete = 7,
    kShutdownInProgress = 8,
    kTimedOut = 9,
    kAborted = 10,
    kBusy = 11,
    kExpired = 12,
    kTryAgain = 13
  };

  Status(int code, const std::string &err) : code_(code), error_(err) {
  }

  Status(int code) : code_(code) {}
  Status() {}

  std::string toString() const {
    return error_;
  }

  int code() const {
    return code_;
  }

  bool ok() const {
    return code_ == Status::kOk;
  }

  bool IsNotFound() const {
    return code_ == Status::kNotFound;
  }

private:
  int code_;
  std::string error_;
};

using RaftClusterID = std::string;

struct RaftServer {
  std::string hostname;
  int port;

  RaftServer() {}
  RaftServer(const std::string &h, int p) : hostname(h), port(p) {}

  bool operator==(const RaftServer& rhs) const {
    return hostname == rhs.hostname && port == rhs.port;
  }
};

class FatalException : public std::exception {
public:
  FatalException(const std::string &m) : msg(m) {}
  virtual ~FatalException() {}

  virtual const char* what() const noexcept {
    return msg.c_str();
  }

private:
  std::string msg;
};

}

#endif
