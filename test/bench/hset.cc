// ----------------------------------------------------------------------
// File: hset.cc
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

#include "raft/RaftContactDetails.hh"
#include "StateMachine.hh"
#include "../test-utils.hh"
#include "bench-utils.hh"
#include <gtest/gtest.h>

using namespace quarkdb;

enum class Mode {
  kDirect = 1,
  kRedisStandalone = 2,
  kConsensus = 3
};

std::string modeToStr(Mode mode) {
  if(mode == Mode::kDirect) return "direct";
  if(mode == Mode::kRedisStandalone) return "standalone";
  if(mode == Mode::kConsensus) return "consensus";
  qdb_throw("should never happen");
}

struct BenchmarkParams {
  int nthreads;
  int events;
  Mode mode;

  BenchmarkParams(int threads, int ev, Mode m) : nthreads(threads), events(ev), mode(m) {}
  operator std::string() const {
    return SSTR("threads" << nthreads << "_events" << events << "_" << modeToStr(mode));
  }
};

class Executor {
public:
  Executor(size_t ev) : events(ev) {}
  virtual ~Executor() {}

  virtual void main(int threadId) = 0;
  virtual std::string describe() = 0;

  size_t getEvents() {
    return events;
  }

protected:
  size_t events;
};

// We provide this class to avoid the overhead of dynamic dispatch with
// virtual functions.
class HsetProvider {
public:
  static inline void handleEventDirect(StateMachine *sm, size_t threadId, size_t eventId) {
    bool created;
    ASSERT_TRUE(sm->hset(SSTR("key-" << eventId), "field", "some_contents", created, 0).ok());
  }

  static inline std::future<qclient::redisReplyPtr> handleEventRedis(qclient::QClient &tunnel, size_t threadId, size_t eventId) {
    return tunnel.exec("hset", SSTR("key-" << eventId), "field", "some_contents");
  }

  static std::string describe() {
    return "HSET";
  }
};

template<typename TestcaseProvider>
class ExecutorHelper : public Executor {
public:
  ExecutorHelper(size_t ev, StateMachine *sm) : Executor(ev), stateMachine(sm) {}
  ExecutorHelper(size_t ev, const RaftServer &srv, const std::string &pw)
  : Executor(ev), server(srv), password(pw) {}

  void main(int threadId) override {
    if(stateMachine) {
      return mainDirect(threadId);
    }
    return mainRedis(threadId);
  }

  void mainDirect(int threadId) {
    while(true) {
      size_t next = nextEvent++;
      if(next > events) break;

      TestcaseProvider::handleEventDirect(stateMachine, threadId, next);
    }
  }

  void mainRedis(int threadId) {
    qclient::Options opts;
    if(!password.empty()) {
      opts.handshake.reset(new qclient::AuthHandshake(password));
    }

    qclient::QClient tunnel(server.hostname, server.port, std::move(opts) );
    while(true) {
      size_t next = nextEvent++;
      if(next > events) break;
      TestcaseProvider::handleEventRedis(tunnel, threadId, next);
    }
    tunnel.exec("ping").get(); // receive all responses
  }

  std::string describe() override {
    return TestcaseProvider::describe();
  }

private:
  RaftServer server;
  StateMachine *stateMachine = nullptr;
  std::atomic<size_t> nextEvent {0};
  std::string password;
};

class Benchmarker {
public:
  static float measureRate(Executor &executor, size_t nthreads) {
    Stopwatch stopwatch(executor.getEvents());

    std::atomic<int64_t> nextEvent {0};
    std::vector<std::thread> threads;
    for(size_t i = 0; i < nthreads; i++) {
      threads.emplace_back(&Executor::main, &executor, i);
    }

    for(size_t i = 0; i < threads.size(); i++) {
      threads[i].join();
    }

    stopwatch.stop();
    return stopwatch.rate();
  }

  void run(Executor &executor, size_t nthreads) {
    qdb_info("Starting benchmark: " << executor.describe());
    float rate = measureRate(executor, nthreads);
    qdb_info("Benchmark has ended. Rate: " << rate << " Hz");
  }
};

template<typename TestcaseProvider>
class BenchmarkHelper : public TestCluster3Nodes {
public:
  ~BenchmarkHelper() {
    if(customPoller) delete customPoller;
    if(customDispatcher) delete customDispatcher;
    if(executor) delete executor;
  }

  void createExecutor(const BenchmarkParams &params) {
    if(params.mode == Mode::kDirect) {
      executor = new ExecutorHelper<TestcaseProvider>(params.events, stateMachine());
    }
    else if(params.mode == Mode::kRedisStandalone) {
      customDispatcher = new RedisDispatcher(*stateMachine());
      customPoller = new Poller(34567, customDispatcher);
      executor = new ExecutorHelper<TestcaseProvider>(params.events, {"localhost", 34567}, "");
    }
    else if(params.mode == Mode::kConsensus) {
      spinup(0); spinup(1); spinup(2);
      RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));
      executor = new ExecutorHelper<TestcaseProvider>(params.events, myself(getLeaderID()), contactDetails()->getPassword() );
    }
  }
protected:
  RedisDispatcher *customDispatcher {nullptr};
  Poller *customPoller {nullptr};
  Executor *executor {nullptr};
};

class hset : public BenchmarkHelper<HsetProvider>, public ::testing::TestWithParam<BenchmarkParams> {
public:
  void run() {
    createExecutor(GetParam());

    Benchmarker benchmarker;
    benchmarker.run(*executor, GetParam().nthreads);
  }
};

static std::vector<BenchmarkParams> generateParams() {
  std::vector<BenchmarkParams> ret;

  for(size_t threads : testconfig.benchmarkThreads.get() ) {
    for(size_t events : testconfig.benchmarkEvents.get() ) {
      for(Mode mode : {Mode::kDirect, Mode::kRedisStandalone, Mode::kConsensus}) {
        ret.emplace_back(threads, events, mode);
      }
    }
  }
  return ret;
}

struct BenchmarkParamsPrinter {
  template <class T>
  std::string operator()(const T& info) const {
    return info.param;
  }
};

INSTANTIATE_TEST_CASE_P(Benchmark,
                        hset,
                        ::testing::ValuesIn(generateParams()),
                        BenchmarkParamsPrinter());

TEST_P(hset, hset) {
  run();
}
