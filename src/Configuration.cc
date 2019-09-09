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

#include "config/ConfigurationReader.hh"
#include "Configuration.hh"
#include "utils/Macros.hh"
#include "utils/FileUtils.hh"
#include "utils/StringUtils.hh"
#include "Utils.hh"

using namespace quarkdb;

bool Configuration::fromFile(const std::string &filename, Configuration &out) {
  qdb_log("Reading configuration file from " << filename);

  std::string contents;
  if(!readFile(filename, contents)) {
    qdb_error("Could not read configuration file: " << filename);
    return false;
  }

  return Configuration::fromString(contents, out);
}

static bool fetchSingle(ConfigurationReader &reader, std::string &dest) {
  reader.advanceWord();

  if(reader.eof()) {
    return false;
  }

  dest = reader.getCurrentWord();

  if(dest.empty()) {
    return false;
  }

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

static bool parseBool(const std::string &buffer, bool &val) {
  if(buffer == "true") {
    val = true;
    return true;
  }
  else if(buffer == "false") {
    val = false;
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

bool Configuration::fromReader(ConfigurationReader &reader, Configuration &out) {

  while(!reader.eof()) {
    std::string current = reader.getCurrentWord();
    bool isMine = StringUtils::startsWith(current, "redis.");

    if(!isMine) {
      reader.advanceLine();
      continue;
    }

    current = std::string(current.begin()+6, current.end());

    bool success = false;
    std::string buffer;

    if(StringUtils::startsWith("mode", current)) {
      success = fetchSingle(reader, buffer) && parseMode(buffer, out.mode);
    }
    else if(StringUtils::startsWith(current, "database")) {
      success = fetchSingle(reader, out.database);
    }
    else if(StringUtils::startsWith(current, "myself")) {
      success = fetchSingle(reader, buffer) && parseServer(buffer, out.myself);
    }
    else if(StringUtils::startsWith(current, "trace")) {
      success = fetchSingle(reader, buffer) && parseTraceLevel(buffer, out.trace);
    }
    else if(StringUtils::startsWith(current, "certificate")) {
      success = fetchSingle(reader, out.certificatePath);
    }
    else if(StringUtils::startsWith(current, "certificate_key")) {
      success = fetchSingle(reader, out.certificateKeyPath);
    }
    else if(StringUtils::startsWith(current, "write_ahead_log")) {
      success = fetchSingle(reader, buffer) && parseBool(buffer, out.writeAheadLog);
    }
    else if(StringUtils::startsWith(current, "password_file")) {
      success = fetchSingle(reader, out.passwordFilePath);
    }
    else if(StringUtils::startsWith(current, "password")) {
      success = fetchSingle(reader, out.password);
    }
    else if(StringUtils::startsWith(current, "require_password_for_localhost")) {
      success = fetchSingle(reader, buffer) && parseBool(buffer, out.requirePasswordForLocalhost);
    }
    else {
      qdb_warn("Error when parsing configuration - unknown option " << quotes(current));
      return false;
    }

    if(!success) {
      qdb_warn("Error when parsing configuration option " << quotes("redis." << current));
      return false;
    }

    reader.advanceLine();
  }

  return out.isValid();
}

bool Configuration::fromString(const std::string &str, Configuration &out) {
  ConfigurationReader reader(str);
  return Configuration::fromReader(reader, out);
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
