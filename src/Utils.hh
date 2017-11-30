// ----------------------------------------------------------------------
// File: Utils.hh
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

#ifndef __QUARKDB_UTILS_H__
#define __QUARKDB_UTILS_H__

#include <iostream>
#include <sstream>
#include <vector>
#include <set>
#include <atomic>
#include <chrono>
#include <mutex>

#include "Common.hh"

namespace quarkdb {


// Controls whether stacktraces are printed on serious errors
// (critical, and exceptions)
// True by default when running a real instance, but false during tests,
// as many error conditions are simulated there, and we'd make the output
// unreadable.
void setStacktraceOnError(bool val);

// Returns a stacktrace if 'stacktrace-on-error' is enabled, empty otherwise.
std::string errorStacktrace(bool crash);

#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&) = delete;   \
  void operator=(const TypeName&) = delete

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()
#define quotes(message) SSTR("'" << message << "'")

extern std::mutex logMutex;
#define TIME_NOW std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()
#define ___log(message) { std::lock_guard<std::mutex> logLock(quarkdb::logMutex); \
  std::cerr << "[" << TIME_NOW << "] " << message << std::endl; }

#define DBG(message) ___log(__FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message)
#define q(message) SSTR("'" << message << "'")

// temporary solution for now
#define qdb_log(message) ___log(message)
#define qdb_event(message) ___log("EVENT: " << message)
#define qdb_critical(message) ___log("CRITICAL: " << message << quarkdb::errorStacktrace(false))
#define qdb_misconfig(message) ___log("MISCONFIGURATION: " << message)

#define qdb_warn(message) ___log("WARNING: " << message)
#define qdb_error(message) ___log("ERROR: " << message)
#define qdb_info(message) ___log("INFO: " << message)
#define qdb_debug(message) if(false) { ___log(message); }

// a serious error has occured signifying a bug in the program logic
#define qdb_throw(message) throw FatalException(SSTR(message << quarkdb::errorStacktrace(true)))
#define qdb_assert(condition) if(!((condition))) throw FatalException(SSTR("assertion violation, condition is not true: " << #condition << quarkdb::errorStacktrace(true)))

bool my_strtoll(const std::string &str, int64_t &ret);
bool my_strtod(const std::string &str, double &ret);
std::vector<std::string> split(std::string data, std::string token);
bool startswith(const std::string &str, const std::string &prefix);
bool parseServer(const std::string &str, RaftServer &srv);
bool parseServers(const std::string &str, std::vector<RaftServer> &servers);
std::string serializeNodes(const std::vector<RaftServer> &nodes);
bool caseInsensitiveEquals(const std::string &str1, const std::string &str2);

inline std::string boolToString(bool b) {
  if(b) return "TRUE";
  return "FALSE";
}

inline std::string vecToString(const std::vector<std::string> &vec) {
  std::ostringstream ss;
  ss << "[";
  for(size_t i = 0; i < vec.size(); i++) {
    ss << vec[i];
    if(i != vec.size()-1) ss << ", ";
  }
  ss << "]";
  return ss.str();
}

// given a vector, checks whether all elements are unique
template<class T>
bool checkUnique(const std::vector<T> &v) {
  for(size_t i = 0; i < v.size(); i++) {
    for(size_t j = 0; j < v.size(); j++) {
      if(i != j && v[i] == v[j]) {
        return false;
      }
    }
  }
  return true;
}

template<class T>
bool contains(const std::vector<T> &v, const T& element) {
  for(size_t i = 0; i <  v.size(); i++) {
    if(v[i] == element) return true;
  }
  return false;
}

template<class T>
bool erase_element(std::vector<T> &v, const T& element) {
  auto it = v.begin();
  while(it != v.end()) {
    if(*it == element) {
      v.erase(it);
      return true;
    }
    it++;
  }
  return false;
}

template<class T>
bool all_identical(const std::vector<T> &v) {
  for(size_t i = 1; i < v.size(); i++) {
    if( !(v[i] == v[i-1]) ) return false;
  }
  return true;
}

}

#endif
