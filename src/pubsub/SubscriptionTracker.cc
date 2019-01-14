// ----------------------------------------------------------------------
// File: SubscriptionTracker.cc
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

#include "SubscriptionTracker.hh"

using namespace quarkdb;

//------------------------------------------------------------------------------
// Add channel
//------------------------------------------------------------------------------
bool SubscriptionTracker::addChannel(const std::string &item) {
	return channels.insert(item).second;
}

//------------------------------------------------------------------------------
// Add pattern
//------------------------------------------------------------------------------
bool SubscriptionTracker::addPattern(const std::string &item) {
	return patterns.insert(item).second;
}

//------------------------------------------------------------------------------
// Check for existence of a channel
//------------------------------------------------------------------------------
bool SubscriptionTracker::hasChannel(const std::string &item) const {
	return channels.find(item) != channels.end();
}

//------------------------------------------------------------------------------
// Check for existence of a pattern
//------------------------------------------------------------------------------
bool SubscriptionTracker::hasPattern(const std::string &item) const {
	return patterns.find(item) != patterns.end();
}
