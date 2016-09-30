// ----------------------------------------------------------------------
// File: link.cc
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

#include "Link.hh"
#include <gtest/gtest.h>

using namespace quarkdb;

TEST(Link, T1) {
  char buffer[1024];

  Link link;
  ASSERT_EQ(link.Send("1234"), 4);
  ASSERT_EQ(link.Recv(buffer, 100, 0), 4);
  ASSERT_EQ("1234", std::string(buffer, 4));

  ASSERT_EQ(link.Send("random_contents"), 15);
  ASSERT_EQ(link.Recv(buffer, 1, 0), 1);
  ASSERT_EQ("r", std::string(buffer, 1));

  ASSERT_EQ(link.Recv(buffer, 3, 0), 3);
  ASSERT_EQ("and", std::string(buffer, 3));

  ASSERT_EQ(link.Recv(buffer, 200, 0), 11);
  ASSERT_EQ("om_contents", std::string(buffer, 11));

  ASSERT_EQ(link.Recv(buffer, 1, 0), 0);

  ASSERT_EQ(link.Close(), 0);
  ASSERT_LT(link.Recv(buffer, 100, 0), 0);
  ASSERT_LT(link.Send("test"), 0);
}

TEST(Link, T2) {
  char buffer[1024];

  Link link;
  ASSERT_EQ(link.Send("adfadfaF"), 8);
  ASSERT_EQ(link.Recv(buffer, 2, 0), 2);
  ASSERT_EQ("ad", std::string(buffer, 2));
}
