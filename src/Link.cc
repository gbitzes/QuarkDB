// ----------------------------------------------------------------------
// File: Link.cc
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
#include <limits>

#include "Link.hh"
#include "Common.hh"
#include "Utils.hh"
#include "utils/Uuid.hh"
#include "utils/Stacktrace.hh"
#include <openssl/ssl.h>
#include <openssl/err.h>

using namespace quarkdb;

namespace {
  // No external linkage
  bool connectionLogging = true;
}

void Link::setConnectionLogging(bool val) {
  connectionLogging = val;
}

using std::placeholders::_1;
using std::placeholders::_2;
using std::placeholders::_3;

qclient::RecvStatus Link::recvStatus(char *buff, int blen, int timeout) {
  int rc = this->rawRecv(buff, blen, timeout);
  if(rc == 0) return qclient::RecvStatus(true, 0, 0); // no pending data to read
  if(rc < 0) return qclient::RecvStatus(false, rc, 0); // connection error
  return qclient::RecvStatus(true, 0, rc); // return data
}

Link::Link(const qclient::TlsConfig &tlsconfig_)
: tlsconfig(tlsconfig_), tlsfilter(tlsconfig, qclient::FilterType::SERVER, std::bind(&Link::recvStatus, this, _1, _2, _3), std::bind(&Link::rawSend, this, _1, _2)) {
  uuid = generateUuid();
}

Link::Link(asio::ip::tcp::socket &socket, qclient::TlsConfig tlsconfig_)
: Link(tlsconfig_) {
  asioSocket = &socket;
  uuid = generateUuid();
  if(connectionLogging) qdb_info("New link from " << describe());
}

Link::Link(int fd_, qclient::TlsConfig tlsconfig_)
: Link(tlsconfig_) {
  uuid = generateUuid();
  fd = fd_;
  fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
}

Link::Link(XrdLink *lp, qclient::TlsConfig tlsconfig_)
: Link(tlsconfig_) {
  uuid = generateUuid();
  host = lp->Host();
  link = lp;

  if(connectionLogging) qdb_info("New link from " << describe());
}

Link::~Link() {
  if(connectionLogging) qdb_info("Shutting down link from " << describe());
  Close();
}

std::string Link::describe() const {
  return SSTR(host << " [" << uuid << "]");
}

void Link::preventXrdLinkClose() {
  xrdLinkCloseDisabled = true;
}

LinkStatus Link::rawRecv(char *buff, int blen, int timeout) {
  if(link) return link->Recv(buff, blen, timeout);
  if(asioSocket) return asioRecv(buff, blen, timeout);
  if(fd >= 0) return fdRecv(buff, blen, timeout);
  return streamRecv(buff, blen, timeout);
}

LinkStatus Link::Recv(char *buff, int blen, int timeout) {
  if(tlsconfig.active) {
    qclient::RecvStatus status = tlsfilter.recv(buff, blen, timeout);
    if(!status.connectionAlive) return -1;
    return status.bytesRead;
  }
  return rawRecv(buff, blen, timeout);
}

LinkStatus Link::Close(int defer) {
  if(tlsconfig.active) tlsfilter.close(defer);
  if(link) {
    if(xrdLinkCloseDisabled) return 1;
    return link->Close(defer);
  }
  if(asioSocket) return asioClose(defer);
  if(fd >= 0) return fdClose(defer);
  return streamClose(defer);
}

LinkStatus Link::rawSend(const char *buff, int blen) {
  if(link) return link->Send(buff, blen);
  if(asioSocket) return asioSend(buff, blen);
  if(fd >= 0) return fdSend(buff, blen);
  return streamSend(buff, blen);
}

LinkStatus Link::Send(const char *buff, int blen) {
  if(dead) return -1;

  LinkStatus ret;
  if(tlsconfig.active) ret = tlsfilter.send(buff, blen);
  ret = rawSend(buff, blen);

  if(ret != blen) {
    dead = true;
    if(ret >= 0) {
      qdb_critical("wrote " << ret << " bytes into Link, even though it should be " << blen);
    }
  }

  return ret;
}

LinkStatus Link::Send(const std::string &str) {
  return Send(str.c_str(), str.size());
}

LinkStatus Link::asioRecv(char *buff, int blen, int timeout) {
  asio::mutable_buffer asioBuff(buff, blen);

  std::error_code ec;
  int len = asioSocket->receive(asioBuff, 0, ec);

  if(ec.value() == 0) {
    return len;
  }
  else if(ec.value() == EAGAIN || ec.value() == EWOULDBLOCK) {
    return 0;
  }

  return - ec.value();
}

LinkStatus Link::asioSend(const char *buff, int blen) {
  asio::error_code ec;
  return asioSocket->send(asio::buffer(buff, blen), 0, ec);
}

LinkStatus Link::asioClose(int defer) {
  asio::error_code ignored_ec;
  asioSocket->shutdown(asio::ip::tcp::socket::shutdown_both, ignored_ec);
  return 0;
}

LinkStatus Link::streamSend(const char *buff, int blen) {
  if(stream.eof()) return -1;
  stream.write(buff, blen);
  return blen;
}

LinkStatus Link::streamClose(int defer) {
  stream.ignore(std::numeric_limits<std::streamsize>::max());
  return 0;
}

LinkStatus Link::streamRecv(char *buff, int blen, int timeout) {
  if(stream.eof()) return -1;

  int totalRead = 0;
  while(true) {
    int rc = stream.readsome(buff, blen);
    totalRead += rc;

    blen -= rc;
    buff += rc;

    if(rc == 0 || blen == 0) break;
  }

  return totalRead;
}

LinkStatus Link::fdRecv(char *buff, int blen, int timeout) {
  int rc = recv(fd, buff, blen, 0);
  if(rc == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) return 0;
  return rc;
}

LinkStatus Link::fdSend(const char *buff, int blen) {
  return send(fd, buff, blen, 0);
}

LinkStatus Link::fdClose(int defer) {
  return close(fd);
}

void Link::overrideHost(const std::string &newhost) {
  host = newhost;
}

bool Link::isLocalhost() const {
  return (
    host == "localhost.localdomain" ||
    host == "localhost"             ||
    host == "127.0.0.1"             ||
    host == "::1"                   ||
    host == "localhost6"            ||
    host == "localhost6.localdomain6"
  );
}
