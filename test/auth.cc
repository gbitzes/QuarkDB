// ----------------------------------------------------------------------
// File: auth.cc
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

#include "utils/FileUtils.hh"
#include "utils/Macros.hh"
#include "auth/AuthenticationDispatcher.hh"
#include "test-utils.hh"
#include <gtest/gtest.h>

using namespace quarkdb;

TEST(FilePermissionChecking, BasicSanity) {
  ASSERT_FALSE(areFilePermissionsSecure(0700));
  ASSERT_FALSE(areFilePermissionsSecure(0777));
  ASSERT_FALSE(areFilePermissionsSecure(0477));
  ASSERT_FALSE(areFilePermissionsSecure(0401));
  ASSERT_FALSE(areFilePermissionsSecure(0455));
  ASSERT_FALSE(areFilePermissionsSecure(0444));
  ASSERT_FALSE(areFilePermissionsSecure(0404));
  ASSERT_FALSE(areFilePermissionsSecure(0440));
  ASSERT_FALSE(areFilePermissionsSecure(0500));
  ASSERT_FALSE(areFilePermissionsSecure(0700));

  ASSERT_TRUE(areFilePermissionsSecure(0400));
}

TEST(ReadPasswordFile, BasicSanity) {
  ASSERT_EQ(system("mkdir -p /tmp/quarkdb-tests/auth/"), 0);
  ASSERT_EQ(system("rm -f /tmp/quarkdb-tests/auth/f1"), 0);

  std::string contents;
  ASSERT_FALSE(readPasswordFile("/tmp/quarkdb-tests/auth/f1", contents));
  ASSERT_FALSE(readPasswordFile("/tmp/quarkdb-tests/auth/non-existing", contents));
  ASSERT_FALSE(readFile("/tmp/quarkdb-tests/auth/non-existing", contents));

  ASSERT_EQ(system("echo 'pickles' > /tmp/quarkdb-tests/auth/f1"), 0);
  ASSERT_EQ(system("chmod 777 /tmp/quarkdb-tests/auth/f1"), 0);
  ASSERT_FALSE(readPasswordFile("/tmp/quarkdb-tests/auth/f1", contents));
  ASSERT_EQ(system("chmod 744 /tmp/quarkdb-tests/auth/f1"), 0);
  ASSERT_FALSE(readPasswordFile("/tmp/quarkdb-tests/auth/f1", contents));
  ASSERT_EQ(system("chmod 700 /tmp/quarkdb-tests/auth/f1"), 0);
  ASSERT_FALSE(readPasswordFile("/tmp/quarkdb-tests/auth/f1", contents));
  ASSERT_EQ(system("chmod 500 /tmp/quarkdb-tests/auth/f1"), 0);
  ASSERT_FALSE(readPasswordFile("/tmp/quarkdb-tests/auth/f1", contents));
  ASSERT_EQ(system("chmod 400 /tmp/quarkdb-tests/auth/f1"), 0);
  ASSERT_TRUE(readPasswordFile("/tmp/quarkdb-tests/auth/f1", contents));

  ASSERT_EQ(system("rm -f /tmp/quarkdb-tests/auth/f1"), 0);
}

TEST(AuthenticationDispatcher, NoPassword) {
  AuthenticationDispatcher dispatcher("");

  bool authorized = false;
  ASSERT_EQ(Formatter::errArgs("AUTH"), dispatcher.dispatch(make_req("AUTH"), authorized));
  ASSERT_TRUE(authorized);

  ASSERT_EQ(Formatter::err("Client sent AUTH, but no password is set").val, dispatcher.dispatch(make_req("AUTH", "test"), authorized).val);
  ASSERT_TRUE(authorized);
}

TEST(AuthenticationDispatcher, TooSmallPassword) {
  ASSERT_THROW(AuthenticationDispatcher("hunter2"), FatalException);
}

TEST(AuthenticationDispatcher, AuthBasicSanity) {
  AuthenticationDispatcher dispatcher("hunter2_hunter2_hunter2_hunter2_hunter2");


  bool authorized = false;
  ASSERT_EQ(Formatter::errArgs("AUTH"), dispatcher.dispatch(make_req("AUTH"), authorized));
  ASSERT_FALSE(authorized);

  ASSERT_EQ(Formatter::err("invalid password"), dispatcher.dispatch(make_req("AUTH", "hunter3"), authorized ));
  ASSERT_FALSE(authorized);

  ASSERT_EQ(Formatter::ok(), dispatcher.dispatch(make_req("AUTH", "hunter2_hunter2_hunter2_hunter2_hunter2"), authorized ));
  ASSERT_TRUE(authorized);
}
