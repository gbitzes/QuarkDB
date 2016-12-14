// -----------------------------------------------------------------------------
// File: XrdQuarkOfs.hh
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
// -----------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
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

#ifndef __XRDQUARK_OFS_HH__
#define __XRDQUARK_OFS_HH__

#include "XrdOfs/XrdOfs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include <string>
#include "Namespace.hh"

XRDQUARKNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Convenience log level to match the ones used in XRootD
//------------------------------------------------------------------------------
struct log
{
  enum lvl {
    emerg   = SYS_LOG_01,
    alert   = SYS_LOG_02,
    crit    = SYS_LOG_03,
    err     = SYS_LOG_04,
    warning = SYS_LOG_05,
    notice  = SYS_LOG_06,
    info    = SYS_LOG_07,
    debug   = SYS_LOG_08
  };
};

//------------------------------------------------------------------------------
//! Get log level as string
//------------------------------------------------------------------------------
static const char*
GetStringLogLvl(log::lvl level)
{
  if (level == log::lvl::debug)
    return "DEBUG";
  else if (level == log::lvl::info)
    return "INFO";
  else if (level == log::lvl::notice)
    return "NOTICE";
  else if (level == log::lvl::warning)
    return "WARNING";
  else if (level == log::lvl::err)
    return "ERR";
  else if (level == log::lvl::crit)
    return "CRIT";
  else if (level == log::lvl::alert)
    return "ALERT";
  else if (level  == log::lvl::emerg)
    return "EMERG";
  else return "UNKNOWN_LVL";
}

//------------------------------------------------------------------------------
//! Class XrdQuarkOfs built on top of XrdOfs
//! @decription: The libXrdQuarkOfs.so is inteded to be used as an OFS library
//! plugin with a vanilla XRootD server.
//------------------------------------------------------------------------------
class XrdQuarkOfs: public XrdOfs
{
public:
  //--------------------------------------------------------------------------
  //! Constuctor
  //--------------------------------------------------------------------------
  XrdQuarkOfs();

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  virtual ~XrdQuarkOfs();

  //--------------------------------------------------------------------------
  //! Configure routine
  //--------------------------------------------------------------------------
  virtual int Configure(XrdSysError& error);

  //--------------------------------------------------------------------------
  //! Get directory object
  //--------------------------------------------------------------------------
  XrdSfsDirectory* newDir(char* user = 0, int MonID = 0) {
    return nullptr;
  }

  //--------------------------------------------------------------------------
  // Get file object
  //--------------------------------------------------------------------------
  XrdSfsFile* newFile(char* user = 0, int MonID = 0) {
    return nullptr;
  }

  //--------------------------------------------------------------------------
  //! Stat function
  //--------------------------------------------------------------------------
  int stat(const char* path,
	   struct stat* buf,
	   XrdOucErrInfo& error,
	   const XrdSecEntity* client,
	   const char* opaque = 0)
  {
    EPNAME("stat");
    return Emsg(epname, error, ENOSYS, epname, path);
  }

  //--------------------------------------------------------------------------
  //! Stat function to retrieve mode
  //--------------------------------------------------------------------------
  int stat(const char* name,
	   mode_t& mode,
	   XrdOucErrInfo& out_error,
	   const XrdSecEntity* client,
	   const char* opaque = 0)
  {
    EPNAME("stat");
    return Emsg(epname, out_error, ENOSYS, epname, name);
  }

  //--------------------------------------------------------------------------
  //! Execute file system command !!! fsctl !!!
  //--------------------------------------------------------------------------
  int fsctl(const int cmd,
	    const char* args,
	    XrdOucErrInfo& out_error,
	    const XrdSecEntity* client);

  //--------------------------------------------------------------------------
  //! Execute file system command !!! FSctl !!!
  //--------------------------------------------------------------------------
  int FSctl(const int cmd,
	    XrdSfsFSctl& args,
	    XrdOucErrInfo& error,
	    const XrdSecEntity* client = 0);

  //--------------------------------------------------------------------------
  //! Chmod function
  //--------------------------------------------------------------------------
  int chmod(const char* path,
	    XrdSfsMode mopde,
	    XrdOucErrInfo& error,
	    const XrdSecEntity* client,
	    const char* opaque = 0)
  {
    EPNAME("chmod");
    return Emsg(epname, error, ENOSYS, epname, path);
  }

  //--------------------------------------------------------------------------
  //! Chksum function
  //--------------------------------------------------------------------------
  int chksum(csFunc func,
	     const char* csName,
	     const char* path,
	     XrdOucErrInfo& error,
	     const XrdSecEntity* client = 0,
	     const char* opaque = 0)
  {
    EPNAME("chksum");
    return Emsg(epname, error, ENOSYS, epname, path);
  }

  //--------------------------------------------------------------------------
  //! Exists function
  //--------------------------------------------------------------------------
  int exists(const char* path,
	     XrdSfsFileExistence& exists_flag,
	     XrdOucErrInfo& error,
	     const XrdSecEntity* client,
	     const char* opaque = 0)
  {
    EPNAME("exists");
    return Emsg(epname, error, ENOSYS, epname, path);
  }

  //--------------------------------------------------------------------------
  //! Create directory
  //--------------------------------------------------------------------------
  int mkdir(const char* dirName,
	    XrdSfsMode Mode,
	    XrdOucErrInfo& out_error,
	    const XrdSecEntity* client,
	    const char* opaque = 0)
  {
    EPNAME("mkdir");
    return Emsg(epname, out_error, ENOSYS, epname, dirName);
  }

  //--------------------------------------------------------------------------
  //! Remove directory
  //--------------------------------------------------------------------------
  int remdir(const char* path,
	     XrdOucErrInfo& error,
	     const XrdSecEntity* client,
	     const char* opaque = 0)
  {
    EPNAME("remdir");
    return Emsg(epname, error, ENOSYS, epname, path);
  }

  //--------------------------------------------------------------------------
  //! Rem file
  //--------------------------------------------------------------------------
  int rem(const char* path,
	  XrdOucErrInfo& error,
	  const XrdSecEntity* client,
	  const char* opaque = 0)
  {
    EPNAME("rem");
    return Emsg(epname, error, ENOSYS, epname, path);
  }

  //--------------------------------------------------------------------------
  //! Rename file
  //--------------------------------------------------------------------------
  int rename(const char* oldName,
	     const char* newName,
	     XrdOucErrInfo& error,
	     const XrdSecEntity* client,
	     const char* opaqueO = 0,
	     const char* opaqueN = 0)
  {
    EPNAME("rename");
    return Emsg(epname, error, ENOSYS, epname, oldName);
  }

  //--------------------------------------------------------------------------
  //! Prepare request
  //--------------------------------------------------------------------------
  int prepare(XrdSfsPrep& pargs,
	      XrdOucErrInfo& error,
	      const XrdSecEntity* client = 0)
  {
    EPNAME("prepare");
    return Emsg(epname, error, ENOSYS, epname, "");
  }

  //--------------------------------------------------------------------------
  //! Truncate file
  //--------------------------------------------------------------------------
  int truncate(const char* path,
	       XrdSfsFileOffset fileOffset,
	       XrdOucErrInfo& error,
	       const XrdSecEntity* client = 0,
	       const char* opaque = 0)
  {
    EPNAME("truncate");
    return Emsg(epname, error, ENOSYS, epname, path);
  }

  //--------------------------------------------------------------------------
  //! getStats function - fake an ok response HERE i.e. do not build and sent
  //! a request to the real MGM
  //--------------------------------------------------------------------------
  int getStats(char* buff, int blen)
  {
    return 0;
  }

private:
  std::string mManagerIp; ///< the IP address of instance
  int mManagerPort;   ///< port on which the current server runs
  int mLogLevel; ///< log level value 0 -7 (LOG_EMERG - LOG_DEBUG)
};


//------------------------------------------------------------------------------
//! Global OFS object
//------------------------------------------------------------------------------
extern XrdQuarkOfs* gOFS;

XRDQUARKNAMESPACE_END

#endif //__XRDQUARK_OFS_HH__
