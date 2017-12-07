// ----------------------------------------------------------------------
// File: KeyConstants.cc
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

#include "KeyConstants.hh"

#define ADD_TO_ALLKEYS(name) allKeys.push_back(name)

namespace quarkdb {

namespace KeyConstants {

std::vector<std::string> allKeys;

class InitializeAllKeys {
public:
  InitializeAllKeys() {
    // I wish there was a way to do this automatically..
    ADD_TO_ALLKEYS(kJournal_CurrentTerm);
    ADD_TO_ALLKEYS(kJournal_LogSize);
    ADD_TO_ALLKEYS(kJournal_LogStart);
    ADD_TO_ALLKEYS(kJournal_ClusterID);
    ADD_TO_ALLKEYS(kJournal_VotedFor);
    ADD_TO_ALLKEYS(kJournal_CommitIndex);
    ADD_TO_ALLKEYS(kJournal_Members);
    ADD_TO_ALLKEYS(kJournal_MembershipEpoch);
    ADD_TO_ALLKEYS(kJournal_PreviousMembers);
    ADD_TO_ALLKEYS(kJournal_PreviousMembershipEpoch);

    ADD_TO_ALLKEYS(kStateMachine_Format);
    ADD_TO_ALLKEYS(kStateMachine_LastApplied);
    ADD_TO_ALLKEYS(kStateMachine_InBulkload);
  }
};

InitializeAllKeys initializer;

}

}
