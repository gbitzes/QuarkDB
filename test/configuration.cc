// ----------------------------------------------------------------------
// File: configuration.cc
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

#include "Configuration.hh"
#include <gtest/gtest.h>

using namespace quarkdb;

TEST(Configuration, T1) {
  Configuration config;
  std::string c;

  c = "if exec xrootd\n"
      "fi\n";

  ASSERT_FALSE(Configuration::fromString(c, config));
}

TEST(Configuration, T2) {
  Configuration config;
  std::string c;

  c = "if exec xrootd\n"
      "xrd.protocol redis:7776 libXrdQuarkDB.so\n"
      "redis.mode raft\n"
      "redis.database /home/user/mydb\n"
      "redis.myself server1:7776\n"
      "redis.trace debug\n"
      "redis.write_ahead_log true\n"
      "redis.password_file /tmp/quarkdb-tests/password-file\n"
      "fi\n";

  ASSERT_TRUE(Configuration::fromString(c, config));
  ASSERT_EQ(config.getMode(), Mode::raft);
  ASSERT_EQ(config.getDatabase(), "/home/user/mydb");
  ASSERT_EQ(config.getMyself(), RaftServer("server1", 7776) );
  ASSERT_EQ(config.getTraceLevel(), TraceLevel::debug);
  ASSERT_EQ(config.getWriteAheadLog(), true);
  ASSERT_EQ(config.getPassword(), "");
  ASSERT_EQ(config.getPasswordFilePath(), "/tmp/quarkdb-tests/password-file");
  ASSERT_THROW(config.extractPasswordOrDie(), FatalException); // file does not exist

  ASSERT_EQ(system("echo 'pickles' > /tmp/quarkdb-tests/password-file"), 0);
  ASSERT_EQ(system("chmod 700 /tmp/quarkdb-tests/password-file"), 0);
  ASSERT_THROW(config.extractPasswordOrDie(), FatalException); // bad permissions

  ASSERT_EQ(system("chmod 400 /tmp/quarkdb-tests/password-file"), 0);
  ASSERT_EQ(config.extractPasswordOrDie(), "pickles\n");

  ASSERT_EQ(system("chmod 700 /tmp/quarkdb-tests/password-file"), 0);
  ASSERT_EQ(system("rm -f /tmp/quarkdb-tests/password-file"), 0);
}

TEST(Configuration, NoPassword) {
  Configuration config;
  std::string c;

  c = "if exec xrootd\n"
      "xrd.protocol redis:7776 libXrdQuarkDB.so\n"
      "redis.mode raft\n"
      "redis.database /home/user/mydb\n"
      "redis.myself server1:7776\n"
      "redis.trace debug\n"
      "redis.write_ahead_log true\n"
      "fi\n";

  ASSERT_TRUE(Configuration::fromString(c, config));
  ASSERT_EQ(config.getMode(), Mode::raft);
  ASSERT_EQ(config.getDatabase(), "/home/user/mydb");
  ASSERT_EQ(config.getMyself(), RaftServer("server1", 7776) );
  ASSERT_EQ(config.getTraceLevel(), TraceLevel::debug);
  ASSERT_EQ(config.getWriteAheadLog(), true);
  ASSERT_EQ(config.getPassword(), "");
  ASSERT_EQ(config.getPasswordFilePath(), "");
  ASSERT_EQ(config.extractPasswordOrDie(), "");
}

TEST(Configuration, PasswordAndPasswordPath) {
  Configuration config;
  std::string c;

  c = "if exec xrootd\n"
      "xrd.protocol redis:7776 libXrdQuarkDB.so\n"
      "redis.mode raft\n"
      "redis.database /home/user/mydb\n"
      "redis.myself server1:7776\n"
      "redis.trace debug\n"
      "redis.write_ahead_log true\n"
      "redis.password_file /etc/super.secure\n"
      "redis.password hunter2\n"
      "fi\n";

  ASSERT_FALSE(Configuration::fromString(c, config));
}

TEST(Configuration, T3) {
  Configuration config;
  std::string c;

  // specifying a raft-only directive when standalone
  c = "if exec xrootd\n"
      "xrd.protocol redis:7776 libXrdQuarkDB.so\n"
      "redis.mode standalone\n"
      "redis.database /home/user/mydb\n"
      "redis.myself server1:7776\n"
      "fi\n";

  ASSERT_FALSE(Configuration::fromString(c, config));
}

TEST(Configuration, T4) {
  Configuration config;
  std::string c;

  c = "if exec xrootd\n"
      "xrd.protocol redis:7776 libXrdQuarkDB.so\n"
      "redis.mode standalone\n"
      "redis.database /home/user/mydb\n"
      "redis.trace info\n"
      "redis.write_ahead_log false\n"
      "redis.password hunter2\n"
      "fi\n";

  ASSERT_TRUE(Configuration::fromString(c, config));
  ASSERT_EQ(config.getMode(), Mode::standalone);
  ASSERT_EQ(config.getDatabase(), "/home/user/mydb");
  ASSERT_EQ(config.getTraceLevel(), TraceLevel::info);
  ASSERT_EQ(config.getWriteAheadLog(), false);
  ASSERT_EQ(config.getPassword(), "hunter2");
  ASSERT_EQ(config.getPasswordFilePath(), "");
  ASSERT_EQ(config.extractPasswordOrDie(), "hunter2");
}

TEST(Configuration, T5) {
  Configuration config;
  std::string c;

  // missing database
  c = "if exec xrootd\n"
      "xrd.protocol redis:7776 libXrdQuarkDB.so\n"
      "redis.mode standalone\n"
      "fi\n";

  ASSERT_FALSE(Configuration::fromString(c, config));
}

TEST(Configuration, T6) {
  Configuration config;
  std::string c;

  // unknown mode
  c = "if exec xrootd\n"
      "xrd.protocol redis:7776 libXrdQuarkDB.so\n"
      "redis.mode something_something\n"
      "redis.database /home/user/mydb\n"
      "fi\n";

  ASSERT_FALSE(Configuration::fromString(c, config));
}

TEST(Configuration, T7) {
  Configuration config;
  std::string c;

  // unknown directive
  c = "if exec xrootd\n"
      "xrd.protocol redis:7776 libXrdQuarkDB.so\n"
      "redis.mode standalone\n"
      "redis.database /home/user/mydb\n"
      "redis.blublu something\n"
      "fi\n";

  ASSERT_FALSE(Configuration::fromString(c, config));
}

TEST(Configuration, T8) {
  Configuration config;
  std::string c;

  // unknown trace level
  c = "if exec xrootd\n"
      "xrd.protocol redis:7776 libXrdQuarkDB.so\n"
      "redis.mode standalone\n"
      "redis.database /home/user/mydb\n"
      "redis.trace wrong\n"
      "fi\n";

  ASSERT_FALSE(Configuration::fromString(c, config));
}

TEST(Configuration, T9) {
  Configuration config;
  std::string c;

  c = "if exec xrootd\n"
      "xrd.protocol redis:7776 libXrdQuarkDB.so\n"
      "redis.mode standalone\n"
      "redis.database /home/user/mydb/\n"
      "redis.trace info\n"
      "fi\n";

  // no trailing slashes in redis.database
  ASSERT_FALSE(Configuration::fromString(c, config));
}

TEST(Configuration, T10) {
  Configuration config;
  std::string c;

  c = "if exec xrootd\n"
      "xrd.protocol redis:7776 libXrdQuarkDB.so\n"
      "redis.mode standalone\n"
      "redis.database /home/user/mydb\n"
      "redis.write_ahead_log qadsfadf\n"
      "fi\n";

  ASSERT_FALSE(Configuration::fromString(c, config));
}
