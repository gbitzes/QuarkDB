// ----------------------------------------------------------------------
// File: Configuration.cc
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

#include <algorithm>

#include <unistd.h>
#include <fcntl.h>

#include "XrdOuc/XrdOucEnv.hh"
#include "Configuration.hh"
#include "utils/Macros.hh"
#include "utils/FileUtils.hh"
#include "Utils.hh"

using namespace quarkdb;

bool Configuration::fromFile(const std::string &filename, Configuration &out) {
  qdb_log("Reading configuration file from " << filename);

  XrdOucEnv myEnv;
  XrdOucStream stream(NULL, getenv("XRDINSTANCE"), NULL, "=====> ");

  int fd;

  if ((fd = open(filename.c_str(), O_RDONLY, 0)) < 0) {
    qdb_log("config: error " << errno << " when opening " << filename);
    return false;
  }

  stream.Attach(fd);
  return Configuration::fromStream(stream, out);
}

static bool fetchSingle(XrdOucStream &stream, std::string &dest) {
  char *val;
  if (!(val = stream.GetWord())) {
    return false;
  }

  dest = val;
  return true;
}

static bool parseMode(const std::string &buffer, Mode &mode) {
  if(buffer == "standalone") {
    mode = Mode::standalone;
  }
  else if(buffer == "raft") {
    mode = Mode::raft;
  }
  else if(buffer == "bulkload") {
    mode = Mode::bulkload;
  }
  else {
    qdb_log("Unknown mode: " << quotes(buffer));
    return false;
  }

  return true;
}

static bool parseBool(const std::string &buffer, bool &enableWriteAheadLog) {
  if(buffer == "true") {
    enableWriteAheadLog = true;
    return true;
  }
  else if(buffer == "false") {
    enableWriteAheadLog = false;
    return true;
  }

  qdb_log("Cannot convert to boolean: " << quotes(buffer));
  return false;
}

static bool parseTraceLevel(const std::string &buffer, TraceLevel &trace) {
  if(buffer == "off") {
    trace = TraceLevel::off;
  }
  else if(buffer == "error") {
    trace = TraceLevel::error;
  }
  else if(buffer == "warn" || buffer == "warning") {
    trace = TraceLevel::warning;
  }
  else if(buffer == "info") {
    trace = TraceLevel::info;
  }
  else if(buffer == "debug" || buffer == "all") {
    trace = TraceLevel::debug;
  }
  else {
    qdb_log("Unknown trace level: " << quotes(buffer));
    return false;
  }

  return true;
}

bool Configuration::fromStream(XrdOucStream &stream, Configuration &out) {
  std::string buffer;
  char *option;

  while ((option = stream.GetMyFirstWord())) {
    bool ismine;
    if ((ismine = !strncmp("redis.", option, 6)) && option[6]) option += 6;

    if(ismine) {
      bool success;

      if(!strcmp("mode", option)) {
        success = fetchSingle(stream, buffer) && parseMode(buffer, out.mode);
      }
      else if(!strcmp("database", option)) {
        success = fetchSingle(stream, out.database);
      }
      else if(!strcmp("myself", option)) {
        success = fetchSingle(stream, buffer) && parseServer(buffer, out.myself);
      }
      else if(!strcmp("trace", option)) {
        success = fetchSingle(stream, buffer) && parseTraceLevel(buffer, out.trace);
      }
      else if(!strcmp("certificate", option)) {
        success = fetchSingle(stream, out.certificatePath);
      }
      else if(!strcmp("certificate_key", option)) {
        success = fetchSingle(stream, out.certificateKeyPath);
      }
      else if(!strcmp("write_ahead_log", option)) {
        success = fetchSingle(stream, buffer) && parseBool(buffer, out.writeAheadLog);
      }
      else if(!strcmp("password_file", option)) {
        success = fetchSingle(stream, out.passwordFilePath);
      }
      else if(!strcmp("password", option)) {
        success = fetchSingle(stream, out.password);
      }
      else if(!strcmp("require_password_for_localhost", option)) {
        success = fetchSingle(stream, buffer) && parseBool(buffer, out.requirePasswordForLocalhost);
      }
      else {
        qdb_warn("Error when parsing configuration - unknown option " << quotes(option));
        return false;
      }

      if(!success) {
        qdb_warn("Error when parsing configuration option " << quotes("redis." << option));
        return false;
      }

    }
  }

  return out.isValid();
}

bool Configuration::fromString(const std::string &str, Configuration &out) {
  XrdOucStream stream(NULL, getenv("XRDINSTANCE"), NULL, "=====> ");

  int fd[2];
  if(pipe(fd) != 0) {
    throw FatalException("unable to create pipe");
  }

  if(write(fd[1], str.c_str(), str.size()) < 0) {
    throw FatalException("unable to write to pipe");
  }

  close(fd[1]);
  stream.Attach(fd[0]);
  return Configuration::fromStream(stream, out);
}

bool Configuration::isValid() {
  if(database.empty()) {
    qdb_log("redis.database must be specified.");
    return false;
  }

  bool raft = (mode == Mode::raft);

  if(raft == myself.hostname.empty()) {
    qdb_log("redis.myself is required when using raft and is incompatible with rocksdb");
    return false;
  }

  if(database[database.size()-1] == '/') {
    qdb_log("redis.database cannot contain trailing slashes");
    return false;
  }

  if(certificatePath.empty() != certificateKeyPath.empty()) {
    qdb_log("Both the TLS certificate and key must be supplied.");
    return false;
  }

  if(!passwordFilePath.empty() && !password.empty()) {
    qdb_log("Cannot both specify redis.password_file and redis.password, choose one or the other");
    return false;
  }

  if(password.empty() && passwordFilePath.empty() && requirePasswordForLocalhost) {
    qdb_log("Cannot require password for localhost, when no password has been set!");
    return false;
  }

  return true;
}

std::string Configuration::extractPasswordOrDie() const {
  qdb_assert(passwordFilePath.empty() || password.empty());

  if(!password.empty()) {
    return password;
  }

  if(passwordFilePath.empty()) {
    return "";
  }

  std::string contents;
  if(!readPasswordFile(passwordFilePath, contents)) {
    qdb_throw("Could not read password file: " << passwordFilePath);
  }

  return contents;
}
