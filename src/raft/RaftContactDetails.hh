// ----------------------------------------------------------------------
// File: RaftContactDetails.hh
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

#ifndef QUARKDB_RAFT_CONTACT_DETAILS_H
#define QUARKDB_RAFT_CONTACT_DETAILS_H

#include "RaftTimeouts.hh"

namespace quarkdb {

using RaftClusterID = std::string;

//------------------------------------------------------------------------------
//! An immutable class containing all auxiliary information required to
//! establish a node-to-node connection.
//------------------------------------------------------------------------------
class RaftContactDetails {
public:
  RaftContactDetails(const RaftClusterID &cid, const RaftTimeouts &t, const std::string &pw)
  : clusterID(cid), timeouts(t), password(pw) { }

  const RaftClusterID& getClusterID() const {
    return clusterID;
  }

  const RaftTimeouts& getRaftTimeouts() const {
    return timeouts;
  }

  const std::string& getPassword() const {
    return password;
  }

private:
  const RaftClusterID clusterID;
  const RaftTimeouts timeouts;
  const std::string password;
};

}

#endif
