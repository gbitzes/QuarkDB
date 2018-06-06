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
