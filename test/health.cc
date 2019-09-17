// ----------------------------------------------------------------------
// File: health.cc
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

#include "health/HealthIndicator.hh"

#include <gtest/gtest.h>
using namespace quarkdb;

TEST(HealthStatus, ToString) {
  ASSERT_EQ(healthStatusAsString(HealthStatus::kGreen), "GREEN");
  ASSERT_EQ(healthStatusAsString(HealthStatus::kYellow), "YELLOW");
  ASSERT_EQ(healthStatusAsString(HealthStatus::kRed), "RED");
}

TEST(HealthStatus, ChooseWorst) {
  ASSERT_EQ(chooseWorstHealth(HealthStatus::kGreen, HealthStatus::kGreen), HealthStatus::kGreen);

  ASSERT_EQ(chooseWorstHealth(HealthStatus::kYellow, HealthStatus::kGreen), HealthStatus::kYellow);
  ASSERT_EQ(chooseWorstHealth(HealthStatus::kGreen, HealthStatus::kYellow), HealthStatus::kYellow);

  ASSERT_EQ(chooseWorstHealth(HealthStatus::kYellow, HealthStatus::kYellow), HealthStatus::kYellow);

  ASSERT_EQ(chooseWorstHealth(HealthStatus::kRed, HealthStatus::kYellow), HealthStatus::kRed);
  ASSERT_EQ(chooseWorstHealth(HealthStatus::kYellow, HealthStatus::kRed), HealthStatus::kRed);

  ASSERT_EQ(chooseWorstHealth(HealthStatus::kRed, HealthStatus::kGreen), HealthStatus::kRed);
  ASSERT_EQ(chooseWorstHealth(HealthStatus::kGreen, HealthStatus::kRed), HealthStatus::kRed);

  ASSERT_EQ(chooseWorstHealth(HealthStatus::kRed, HealthStatus::kRed), HealthStatus::kRed);
}

TEST(HealthIndicator, BasicSanity) {
  HealthIndicator ind1(HealthStatus::kGreen, "AVAILABLE-SPACE", "120 GB");
  ASSERT_EQ(ind1.getStatus(), HealthStatus::kGreen);
  ASSERT_EQ(ind1.getDescription(), "AVAILABLE-SPACE");
  ASSERT_EQ(ind1.getMessage(), "120 GB");

  ASSERT_EQ(ind1.toString(), "[GREEN] AVAILABLE-SPACE 120 GB");
}

