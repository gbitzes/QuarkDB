// ----------------------------------------------------------------------
// File: Tunnel.cc
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

#include "Tunnel.hh"
#include <sys/eventfd.h>
#include <unistd.h>
#include <string.h>
#include <poll.h>

using namespace quarkdb;

//------------------------------------------------------------------------------
// The intercepts machinery
//------------------------------------------------------------------------------

std::map<std::pair<std::string, int>, std::string> Tunnel::intercepts;
std::mutex Tunnel::interceptsMutex;

void Tunnel::addIntercept(const std::string &hostname, int port, const std::string &unixSocket) {
  std::lock_guard<std::mutex> lock(interceptsMutex);
  intercepts[std::make_pair(hostname, port)] = unixSocket;
}

void Tunnel::clearIntercepts() {
  std::lock_guard<std::mutex> lock(interceptsMutex);
  intercepts.clear();
}

//------------------------------------------------------------------------------
// Tunnel class implementation
//------------------------------------------------------------------------------

void Tunnel::startEventLoop() {
  asyncContext = nullptr;
  writeEventFD.reset();

  this->connect();
  eventLoopThread = std::thread(&Tunnel::eventLoop, this);
}

Tunnel::Tunnel(const std::string &host_, const int port_, RedisRequest handshake)
: host(host_), port(port_), handshakeCommand(handshake) {

  //----------------------------------------------------------------------------
  // If this (host, port) pair is being intercepted, connect to the designated
  // unix socket instead.
  //----------------------------------------------------------------------------
{
  std::lock_guard<std::mutex> lock(interceptsMutex);
  auto it = intercepts.find(std::make_pair(host, port));
  if(it != intercepts.end()) {
    unixSocket = it->second;
  }
}

  startEventLoop();
}

Tunnel::~Tunnel() {
  shutdown = true;
  while(threadsAlive != 0) {
    notifyWrite();
  }
  eventLoopThread.join();
  disconnect();
}

static void add_write_callback(void *privdata) {
  Tunnel *tunnel = (Tunnel*) privdata;
  tunnel->notifyWrite();
}

static void del_write_callback(void *privdata) {
  Tunnel *tunnel = (Tunnel*) privdata;
  tunnel->removeWriteNotification();
}

static redisReply* dupReplyObject(redisReply* reply) {
    redisReply* r = (redisReply*)calloc(1, sizeof(*r));
    memcpy(r, reply, sizeof(*r));
    if(REDIS_REPLY_ERROR==reply->type || REDIS_REPLY_STRING==reply->type || REDIS_REPLY_STATUS==reply->type) //copy str
    {
        r->str = (char*)malloc(reply->len+1);
        memcpy(r->str, reply->str, reply->len);
        r->str[reply->len] = '\0';
    }
    else if(REDIS_REPLY_ARRAY==reply->type) //copy array
    {
        r->element = (redisReply**)calloc(reply->elements, sizeof(redisReply*));
        memset(r->element, 0, r->elements*sizeof(redisReply*));
        for(uint32_t i=0; i<reply->elements; ++i)
        {
            if(NULL!=reply->element[i])
            {
                if( NULL == (r->element[i] = dupReplyObject(reply->element[i])) )
                {
                    //clone child failed, free current reply, and return NULL
                        freeReplyObject(r);
                    return NULL;
                }
            }
        }
    }
    return r;
}

static void async_future_callback(redisAsyncContext *c, void *reply, void *privdata) {
  redisReply *rreply = (redisReply*) reply;
  std::promise<redisReplyPtr> *promise = (std::promise<redisReplyPtr>*) privdata;

  if(reply) {
    promise->set_value(redisReplyPtr(dupReplyObject(rreply), freeReplyObject));
  }
  else {
    promise->set_value(redisReplyPtr());
  }
  delete promise;
}

void Tunnel::notifyWrite() {
  writeEventFD.notify();
}

void Tunnel::removeWriteNotification() {
  writeEventFD.reset();
}

void Tunnel::disconnect() {
  if(asyncContext) {
    redisAsyncDisconnect(asyncContext);
    asyncContext = nullptr;
  }
}

void Tunnel::connect() {
  std::unique_lock<std::recursive_mutex> lock(asyncMutex);
  disconnect();
  // TODO: figure out what I have to do to free the async context

  if(unixSocket.empty()) {
    asyncContext = redisAsyncConnect(host.c_str(), port);
  }
  else {
    asyncContext = redisAsyncConnectUnix(unixSocket.c_str());
  }

  asyncContext->ev.addWrite = add_write_callback;
  asyncContext->ev.delWrite = del_write_callback;
  asyncContext->ev.data = this;

  if(!handshakeCommand.empty()) {
    execute(handshakeCommand);
  }

  lock.unlock();

}

void Tunnel::eventLoop() {
  ScopedAdder<int64_t> adder(threadsAlive);

  std::chrono::milliseconds backoff(1);
  while(true) {
    struct pollfd polls[2];
    polls[0].fd = writeEventFD.getFD();
    polls[0].events = POLLIN;

    polls[1].fd = asyncContext->c.fd;
    polls[1].events = POLLIN;

    while(!asyncContext->err) {
      poll(polls, 2, -1);
      if(shutdown) return;

      std::unique_lock<std::recursive_mutex> lock(asyncMutex);

      if(asyncContext->err) {
        break;
      }

      // legit connection, reset backoff
      backoff = std::chrono::milliseconds(1);

      if(polls[0].revents != 0) {
        redisAsyncHandleWrite(asyncContext);
      }
      else if(polls[1].revents != 0) {
        redisAsyncHandleRead(asyncContext);
      }
    }

    if(shutdown) return;
    // dropped connection, wait before retrying with an exponential backoff
    std::this_thread::sleep_for(backoff);
    if(backoff < std::chrono::milliseconds(1024)) {
      backoff *= 2;
    }
    this->connect();
  }
}

std::future<redisReplyPtr> Tunnel::execute(const RedisRequest &req) {
  const char *cstr[req.size()];
  size_t sizes[req.size()];

  for(size_t i = 0; i < req.size(); i++) {
    cstr[i] = req[i].c_str();
    sizes[i] = req[i].size();
  }

  return execute(req.size(), cstr, sizes);
}

std::future<redisReplyPtr> Tunnel::execute(size_t nchunks, const char **chunks, const size_t *sizes) {
  std::lock_guard<std::recursive_mutex> lock(asyncMutex);

  if(asyncContext && !asyncContext->err) {
    std::promise<redisReplyPtr>* prom = new std::promise<redisReplyPtr>();
    if(redisAsyncCommandArgv(asyncContext, async_future_callback, prom, nchunks, chunks, sizes) != REDIS_OK) {
      prom->set_value(redisReplyPtr());
      delete prom;
    }
    return prom->get_future();
  }

  std::promise<redisReplyPtr> prom;
  prom.set_value(redisReplyPtr());
  return prom.get_future();
}
