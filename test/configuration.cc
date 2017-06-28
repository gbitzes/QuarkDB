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
      "fi\n";

  ASSERT_TRUE(Configuration::fromString(c, config));
  ASSERT_EQ(config.getMode(), Mode::raft);
  ASSERT_EQ(config.getDatabase(), "/home/user/mydb");
  ASSERT_EQ(config.getMyself(), RaftServer("server1", 7776) );
  ASSERT_EQ(config.getTraceLevel(), TraceLevel::debug);
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
      "fi\n";

  ASSERT_TRUE(Configuration::fromString(c, config));
  ASSERT_EQ(config.getMode(), Mode::standalone);
  ASSERT_EQ(config.getDatabase(), "/home/user/mydb");
  ASSERT_EQ(config.getTraceLevel(), TraceLevel::info);
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
