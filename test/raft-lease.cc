// ----------------------------------------------------------------------
// File: raft-lease.cc
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

#include "raft/RaftLease.hh"
#include <gtest/gtest.h>

using namespace quarkdb;
using namespace std::chrono;

TEST(Raft_Lease, basic_sanity_with_2_endpoints) {
  std::vector<RaftServer> targets;
  targets.emplace_back("localhost", 1234);

  steady_clock::duration leaseDuration(seconds(1));
  RaftLease lease(targets, leaseDuration);

  RaftLastContact &t1 = lease.getHandler(RaftServer("localhost", 1234));

  steady_clock::time_point p0; // 0 ms
  steady_clock::time_point p1 = p0 + milliseconds(500);

  t1.heartbeat(p1);

  ASSERT_EQ(lease.getDeadline(), p1 + leaseDuration);
  ASSERT_EQ(lease.getShakyQuorumDeadline(), p1 + leaseDuration);

  steady_clock::time_point p2 = p0 + milliseconds(501);

  t1.heartbeat(p2);
  ASSERT_EQ(lease.getDeadline(), p2 + leaseDuration);
  ASSERT_EQ(lease.getShakyQuorumDeadline(), p2 + leaseDuration);
}

TEST(Raft_Lease, basic_sanity_with_3_endpoints) {
  std::vector<RaftServer> targets;

  targets.emplace_back("localhost", 1234);
  targets.emplace_back("example.com", 5678);

  steady_clock::duration leaseDuration(seconds(1));
  RaftLease lease(targets, leaseDuration);

  RaftLastContact &t1 = lease.getHandler(RaftServer("localhost", 1234));
  RaftLastContact &t2 = lease.getHandler(RaftServer("example.com", 5678));

  steady_clock::time_point p0; // 0 ms
  steady_clock::time_point p1 = p0 + milliseconds(500);
  steady_clock::time_point p2 = p0 + milliseconds(1000);

  t1.heartbeat(p1);
  t2.heartbeat(p2);

  ASSERT_EQ(lease.getDeadline(), p2 + leaseDuration);
  ASSERT_EQ(lease.getShakyQuorumDeadline(), p1 + leaseDuration);

  t1.heartbeat(p0 + milliseconds(750));
  ASSERT_EQ(lease.getDeadline(), p2 + leaseDuration);
  ASSERT_EQ(lease.getShakyQuorumDeadline(), p0 + milliseconds(750) + leaseDuration);

  steady_clock::time_point p3 = p0 + milliseconds(1500);
  t1.heartbeat(p3);
  ASSERT_EQ(lease.getDeadline(), p3 + leaseDuration);
  ASSERT_EQ(lease.getShakyQuorumDeadline(), p2 + leaseDuration);

  steady_clock::time_point p4 = p0 + milliseconds(2000);
  t2.heartbeat(p4);
  ASSERT_EQ(lease.getDeadline(), p4 + leaseDuration);
  ASSERT_EQ(lease.getShakyQuorumDeadline(), p3 + leaseDuration);
}

TEST(Raft_Lease, basic_sanity_with_5_endpoints) {
  std::vector<RaftServer> targets;

  targets.emplace_back("localhost", 1234);
  targets.emplace_back("example.com", 5678);
  targets.emplace_back("localhost", 9999);
  targets.emplace_back("example.com", 9999);

  steady_clock::duration leaseDuration(seconds(1));
  RaftLease lease(targets, leaseDuration);

  RaftLastContact &t1 = lease.getHandler(RaftServer("localhost", 1234));
  RaftLastContact &t2 = lease.getHandler(RaftServer("example.com", 5678));
  RaftLastContact &t3 = lease.getHandler(RaftServer("localhost", 9999));
  RaftLastContact &t4 = lease.getHandler(RaftServer("example.com", 9999));

  steady_clock::time_point p0; // 0 ms
  steady_clock::time_point p1 = p0 + milliseconds(500);
  steady_clock::time_point p2 = p0 + milliseconds(750);
  steady_clock::time_point p3 = p0 + milliseconds(800);
  steady_clock::time_point p4 = p0 + milliseconds(900);

  t1.heartbeat(p1);
  t2.heartbeat(p2);
  t3.heartbeat(p3);
  t4.heartbeat(p4);

  ASSERT_EQ(lease.getDeadline(), p3 + leaseDuration);

  t1.heartbeat(p0 + milliseconds(600));
  ASSERT_EQ(lease.getDeadline(), p3 + leaseDuration);
  ASSERT_EQ(lease.getShakyQuorumDeadline(), p2 + leaseDuration);

  t1.heartbeat(p0 + milliseconds(700));
  ASSERT_EQ(lease.getDeadline(), p3 + leaseDuration);
  ASSERT_EQ(lease.getShakyQuorumDeadline(), p2 + leaseDuration);

  steady_clock::time_point p5 = p0 + milliseconds(801);
  t1.heartbeat(p5);
  ASSERT_EQ(lease.getDeadline(), p5 + leaseDuration);
  ASSERT_EQ(lease.getShakyQuorumDeadline(), p3 + leaseDuration);

  steady_clock::time_point p6 = p0 + milliseconds(10000);
  t2.heartbeat(p6);
  ASSERT_EQ(lease.getDeadline(), p4 + leaseDuration);
  ASSERT_EQ(lease.getShakyQuorumDeadline(), p5 + leaseDuration);
}
