// ----------------------------------------------------------------------
// File: KeyConstants.hh
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

#ifndef __QUARKDB_KEY_CONSTANTS_H__
#define __QUARKDB_KEY_CONSTANTS_H__

#include <string>
#include <vector>

namespace quarkdb {

namespace KeyConstants {
  constexpr char kJournal_CurrentTerm[]              = "RAFT_CURRENT_TERM";
  constexpr char kJournal_LogSize[]                  = "RAFT_LOG_SIZE";
  constexpr char kJournal_LogStart[]                 = "RAFT_LOG_START";
  constexpr char kJournal_ClusterID[]                = "RAFT_CLUSTER_ID";
  constexpr char kJournal_VotedFor[]                 = "RAFT_VOTED_FOR";
  constexpr char kJournal_CommitIndex[]              = "RAFT_COMMIT_INDEX";
  constexpr char kJournal_Members[]                  = "RAFT_MEMBERS";
  constexpr char kJournal_MembershipEpoch[]          = "RAFT_MEMBERSHIP_EPOCH";
  constexpr char kJournal_PreviousMembers[]          = "RAFT_PREVIOUS_MEMBERS";
  constexpr char kJournal_PreviousMembershipEpoch[]  = "RAFT_PREVIOUS_MEMBERSHIP_EPOCH";

  constexpr char kStateMachine_Format[]              = "__format";
  constexpr char kStateMachine_LastApplied[]         = "__last-applied";

  extern std::vector<std::string> allKeys;
};


}

#endif
