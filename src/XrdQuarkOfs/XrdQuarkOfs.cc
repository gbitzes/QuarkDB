// -----------------------------------------------------------------------------
// File: XrdQuarkOfs.cc
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

#include "XrdQuarkOfs.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOss/XrdOssApi.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdNet/XrdNetIF.hh"
#include "XrdVersion.hh"
#include "../Common.hh"
#include <sstream>
#include "../Utils.hh"

// The global OFS handle
quarkdb::XrdQuarkOfs* quarkdb::gOFS;
extern XrdSysError OfsEroute;
extern XrdOfs* XrdOfsFS;
XrdVERSIONINFO(XrdSfsGetFileSystem, QuarkOfs);
char g_logstring[2048];

//------------------------------------------------------------------------------
// Log wrapping function
//------------------------------------------------------------------------------
static
void log_fn(quarkdb::log::lvl level, const char *format, ...) {
  va_list args;
  va_start(args, format);
  vsnprintf(g_logstring, 2048, format, args);
  va_end(args);
  OfsEroute.Log(level, quarkdb::GetStringLogLvl(level), g_logstring);
}

//------------------------------------------------------------------------------
// Filesystem Plugin factory function
//------------------------------------------------------------------------------
extern "C"
{
  XrdSfsFileSystem* XrdSfsGetFileSystem(XrdSfsFileSystem* native_fs,
					XrdSysLogger* lp,
					const char* configfn)
  {
    // Do the herald thing
    OfsEroute.SetPrefix("QuarkOfs_");
    OfsEroute.logger(lp);
    XrdOucString version = "QuarkOfs (Object Storage File System) ";
    version += XrdVERSION;
    OfsEroute.Say("++++++ (c) 2016 CERN/IT-DSS ", version.c_str());
    // Initialize the subsystems
    quarkdb::gOFS = new quarkdb::XrdQuarkOfs();
    quarkdb::gOFS->ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);

    if (quarkdb::gOFS->Configure(OfsEroute)) {
      return 0;
    }

    XrdOfsFS = quarkdb::gOFS;
    return quarkdb::gOFS;
  }
}

using namespace qclient;

XRDQUARKNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdQuarkOfs::XrdQuarkOfs()
{
  // empty
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdQuarkOfs::~XrdQuarkOfs()
{
  // empty
}

//------------------------------------------------------------------------------
// Configuration of the OFS plugin
//------------------------------------------------------------------------------
int
XrdQuarkOfs::Configure(XrdSysError& error)
{
  tunnel = new QClient("localhost", myPort);
  error.setMsgMask(log::lvl::info);
  return 0;
}

//------------------------------------------------------------------------------
// Execute file system command !!! fsctl !!!
//------------------------------------------------------------------------------
int
XrdQuarkOfs::fsctl(const int cmd, const char* args, XrdOucErrInfo& out_error,
		   const XrdSecEntity* client)
{
  // TODO (esindril): implement this
  EPNAME("fsctl");
  return Emsg(epname, out_error, ENOSYS, epname, "");
}

static char* redisReplyToStr(redisReply *reply) {
  if(reply->type == REDIS_REPLY_STRING) {
    char* buffer = new char[reply->len + 25];

    int len = sprintf(buffer, "$%d\r\n", reply->len);

    if(reply->len != 0) {
      memcpy(buffer+len, reply->str, reply->len); // we might have embedded null bytes
      sprintf(buffer+len+reply->len, "\r\n");
    }
    return buffer;
  }
  else if(reply->type == REDIS_REPLY_ERROR || reply->type == REDIS_REPLY_STATUS) {
    char* buffer = new char[reply->len + 25];
    char prefix = '-';
    if(reply->type == REDIS_REPLY_STATUS) prefix = '+';
    sprintf(buffer, "%c%s\r\n", prefix, reply->str);
    return buffer;
  }
  else if(reply->type == REDIS_REPLY_INTEGER) {
    char* buffer = new char[50];
    sprintf(buffer, ":%lld\r\n", reply->integer);
    return buffer;
  }
  else if(reply->type == REDIS_REPLY_NIL) {
    char* buffer = new char[10];
    sprintf(buffer, "$-1\r\n");
    return buffer;
  }
  else if(reply->type == REDIS_REPLY_ARRAY) {
    std::ostringstream ss;
    ss << "*" << reply->elements;

    for(size_t i = 0; i < reply->elements; i++) {
      char* buffer = redisReplyToStr(reply->element[i]);
      ss << buffer;
      delete buffer;
    }

    char *buffer = new char[ss.tellp()];
    memcpy(buffer, ss.str().c_str(), ss.tellp());
    return buffer;
  }

  qdb_throw("should never happen");
}

//------------------------------------------------------------------------------
// Execute file system command !!! FSctl !!!
//------------------------------------------------------------------------------
int
XrdQuarkOfs::FSctl(const int cmd, XrdSfsFSctl& args, XrdOucErrInfo& error,
		   const XrdSecEntity* client)
{
  log_fn(log::lvl::info, "arg1:%s arg1len:%i arg2:%s arg2len:%i", args.Arg1,
	args.Arg1Len, args.Arg2, args.Arg2Len);

  redisReplyPtr rep = tunnel->execute(args.Arg1, args.Arg1Len).get();

  if(!rep) {
    return SFS_ERROR;
  }

  char* response = redisReplyToStr(rep.get());
  XrdOucBuffer* xrd_buff = new XrdOucBuffer(response, strlen(response));
  error.setErrInfo(xrd_buff->BuffSize(), xrd_buff);
  return SFS_DATA;
}

XRDQUARKNAMESPACE_END
