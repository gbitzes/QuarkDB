// ----------------------------------------------------------------------
// File: Transaction.hh
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

#ifndef QUARKDB_REDIS_TRANSACTION_H
#define QUARKDB_REDIS_TRANSACTION_H

#include "../RedisRequest.hh"

namespace quarkdb {

class Transaction {
public:
  Transaction();
  ~Transaction();

  explicit Transaction(RedisRequest &&req);

  void push_back(RedisRequest &&req);
  bool containsWrites() const {
    return hasWrites;
  }

  std::string serialize() const;
  bool deserialize(const PinnedBuffer &src);
  bool deserialize(const RedisRequest &req);

  RedisRequest& operator[](size_t i) {
    return requests[i];
  }

  const RedisRequest& operator[](size_t i) const {
    return requests[i];
  }

  bool operator==(const Transaction &rhs) const {
    return requests == rhs.requests;
  }

  template<typename... Args>
  void emplace_back(Args&&... args) {
    requests.emplace_back( RedisRequest { args ... } );
    checkNthCommandForWrites();
  }

  size_t size() const {
    return requests.size();
  }

  bool empty() const {
    return (requests.size() == 0u);
  }

  void clear();

  bool isPhantom() const {
    return phantom;
  }

  void setPhantom(bool val) {
    phantom = val;
  }

  RedisRequest toRedisRequest() const;
  std::string getFusedCommand() const;
  void fromRedisRequest(const RedisRequest &req);
  std::string toPrintableString() const;

  //----------------------------------------------------------------------------
  // How many responses is the client to this transaction expecting?
  // - size() if this is a phantom transaction. The client cannot possibly
  //   know we're batching the requests in the background, and will be utterly
  //   confused if we provide fewer responses than actual requests sent.
  // - Just one, otherwise. The client issued a real transaction, and knows to
  //   expect just a single (vector) response.
  //----------------------------------------------------------------------------
  int64_t expectedResponses() const {
    if(phantom) {
      return requests.size();
    }

    return 1;
  }

private:
  void checkNthCommandForWrites(int n = -1);

  bool hasWrites = false;
  bool phantom = false;
  std::vector<RedisRequest> requests;
  std::string typeInString() const;
};

}

#endif
