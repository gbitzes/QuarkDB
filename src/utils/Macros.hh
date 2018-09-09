// ----------------------------------------------------------------------
// File: Macros.hh
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

#ifndef QUARKDB_MACROS_HH
#define QUARKDB_MACROS_HH

#include <mutex>
#include <iostream>
#include <sstream>

namespace quarkdb {

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
#define qdb_fatal(message) { ___log(quarkdb::errorStacktrace(true) << std::endl << std::endl); ___log("FATAL: " << message); std::quick_exit(1); }

#define qdb_warn(message) ___log("WARNING: " << message)
#define qdb_error(message) ___log("ERROR: " << message)
#define qdb_info(message) ___log("INFO: " << message)
#define qdb_debug(message) if(false) { ___log(message); }

// a serious error has occured signifying a bug in the program logic
#define qdb_throw(message) throw FatalException(SSTR(message << quarkdb::errorStacktrace(true)))
#define qdb_assert(condition) if(!((condition))) throw FatalException(SSTR("assertion violation, condition is not true: " << #condition << quarkdb::errorStacktrace(true)))

// Always inline this function.
#define QDB_ALWAYS_INLINE __attribute__((always_inline))

}

#endif
