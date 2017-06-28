// ----------------------------------------------------------------------
// File: XrdPlugin.cc
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

#include <iostream>

#include "Xrd/XrdProtocol.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdVersion.hh"

#include "XrdQuarkDB.hh"

extern "C" {
XrdProtocol *XrdgetProtocol(const char *pname, char *parms,
                            XrdProtocol_Config *pi) {
  pi->eDest->Say("Copr. 2016 CERN");
  pi->eDest->Say("++++++ quarkdb server initialization started");

  // check if the given configuration file is valid
  if(!quarkdb::XrdQuarkDB::Configure(parms, pi)) {
    pi->eDest->Say("------ quarkdb protocol plugin initialization failed.");
    return 0;
  }

  XrdProtocol *protocol = (XrdProtocol*) new quarkdb::XrdQuarkDB(false);
  pi->eDest->Say("------ quarkdb protocol plugin initialization completed.");
  return protocol;
}
}


XrdVERSIONINFO(XrdgetProtocol, xrdquarkdb);

//------------------------------------------------------------------------------
// This function is called early on to determine the port we need to use. The
// default is ostensibly 6379 but can be overriden; which we allow.
//------------------------------------------------------------------------------

XrdVERSIONINFO(GetProtocolPort, xrdquarkdb);

extern "C" {
int GetProtocolPort(const char *pname, char *parms, XrdProtocol_Config *pi) {
  if (pi->Port < 0) return 6379;
  return pi->Port;
}
}
