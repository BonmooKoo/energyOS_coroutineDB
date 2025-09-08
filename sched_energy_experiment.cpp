// sched_energy_experiment.cpp
// Build:
/*
g++ -std=gnu++17 -O2 sched_energy_experiment.cpp -o sched_energy \
  -I/opt/boost-1.60/include \
  -L/opt/boost-1.60/lib \
  -Wl,-rpath,/opt/boost-1.60/lib \
  -lboost_coroutine -lboost_context -lboost_system -lpthread
taskset -c 0-3 ./sched_energy --threads 4 --coros 16 --rps 1000 --duration 60 --service-us 200
taskset -c 0-1 ./sched_energy --threads 1 --coros 64 --rps 1000 --duration 60 --service-us 200
*/
//
// Purpose:
//   Measure power difference between (A) 4 cores x 16 coroutines each
//   vs (B) 1 core x 64 coroutines, under the SAME workload (RPS, service time).
//
//   This is a self-contained harness (no RDMA). It simulates incoming requests
//   at a given RPS and processes them in coroutine workers. Use taskset
//   to pin to specific cores for each experiment.

#include <boost/coroutine/symmetric_coroutine.hpp>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>
#include <unistd.h>
#include <sys/syscall.h>
#include <pthread.h>

using namespace std::chrono;

// ----------------- Global control -----------------
static std::atomic<bool> g_running{true};
static std::atomic<uint64_t> g_req_id{0};
static std::atomic<uint64_t> g_done{0};

// ----------------- Request + queue -----------------
struct Request { uint64_t id; uint64_t ts_ns; };

class MPMCQueue {
  mutable std::mutex m_;
  std::deque<Request> q_;
public:
  bool try_pop(Request& out){
    std::lock_guard<std::mutex> lk(m_);
    if (q_.empty()) return false;
    out = std::move(q_.front());
    q_.pop_front();
    return true;
  }
  void push(Request&& r){
    std::lock_guard<std::mutex> lk(m_);
    q_.push_back(std::move(r));
  }
  size_t size() const {
    std::lock_guard<std::mutex> lk(m_);
    return q_.size();
  }
} g_rx;

// ----------------- Helpers -----------------
using coro_t = boost::coroutines::symmetric_coroutine<void>;
using Call   = coro_t::call_type;
using Yield  = coro_t::yield_type;

static inline void cpu_relax(){
#if defined(__x86_64__)
  asm volatile("pause" ::: "memory");
#else
  std::this_thread::yield();
#endif
}

void busy_for_us(int us){
  auto t0 = steady_clock::now();
  while (duration_cast<microseconds>(steady_clock::now()-t0).count() < us) {
    cpu_relax();
  }
}

void pin_to_cpu(int cpu){
  cpu_set_t set; CPU_ZERO(&set); CPU_SET(cpu, &set);
  (void)pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

// ----------------- Thread ctx + workers -----------------
struct ThreadCtx {
  int tid;
  int coros;
  int service_us;
  std::vector<std::unique_ptr<Call>> workers;
  explicit ThreadCtx(int tid_, int c_, int sus) : tid(tid_), coros(c_), service_us(sus) {}
};

void make_worker(ThreadCtx& ctx){
  auto fn = [&](Yield& y){
    Request req{};
    while (g_running.load(std::memory_order_relaxed)) {
      if (g_rx.try_pop(req)) {
        // simulate CPU work for service_us
        busy_for_us(ctx.service_us);
        g_done.fetch_add(1, std::memory_order_relaxed);
        y();  // cooperative yield
      } else {
        // no work right now
        y();
      }
    }
  };
  ctx.workers.emplace_back(new Call(fn));
}

void scheduler_loop(ThreadCtx& ctx){
  pin_to_cpu(ctx.tid);
  for (int i=0;i<ctx.coros;i++) make_worker(ctx);

  // simple round-robin across workers with a moving cursor
  size_t n = ctx.workers.size();
  size_t cursor = 0;
  const int RUN_BATCH_MAX = 64; // tune freely

  while (g_running.load(std::memory_order_relaxed)) {
    int ran = 0;
    for (; ran < RUN_BATCH_MAX && n>0; ++ran) {
      auto& w = ctx.workers[cursor];
      cursor = (cursor + 1) % n;
      if (*w) { (*w)(); }
    }
    if (ran == 0) std::this_thread::yield();
  }
}

// ----------------- Producer (RPS) -----------------
void producer_rps(int rps, int seconds){
  auto period = nanoseconds( (long long)(1e9 / (double)rps) );
  auto start = steady_clock::now();
  auto next  = start;
  auto endt  = start + seconds*1s;
  while (steady_clock::now() < endt && g_running.load(std::memory_order_relaxed)) {
    Request r{ g_req_id.fetch_add(1, std::memory_order_relaxed),
               (uint64_t)duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count() };
    g_rx.push(std::move(r));
    next += period;
    auto now = steady_clock::now();
    if (next > now) std::this_thread::sleep_until(next);
    else next = now; // catch up if lagging
  }
}

// ----------------- Main -----------------
int main(int argc, char** argv){
  // Defaults
  int threads=4;                // use 1 for consolidated case
  int coros_per_thread=16;      // use 64 for 1-thread case
  int rps=1000;                 // incoming requests per second
  int duration_s=30;            // run duration
  int service_us=200;           // per-request CPU busy time

  // CLI parse (very basic)
  for (int i=1;i<argc;i++){
    if (!strcmp(argv[i],"--threads") && i+1<argc) threads=atoi(argv[++i]);
    else if (!strcmp(argv[i],"--coros") && i+1<argc) coros_per_thread=atoi(argv[++i]);
    else if (!strcmp(argv[i],"--rps") && i+1<argc) rps=atoi(argv[++i]);
    else if (!strcmp(argv[i],"--duration") && i+1<argc) duration_s=atoi(argv[++i]);
    else if (!strcmp(argv[i],"--service-us") && i+1<argc) service_us=atoi(argv[++i]);
  }

  std::cout << "threads="<<threads
            << " coros_per_thread="<<coros_per_thread
            << " rps="<<rps
            << " duration="<<duration_s<<"s"
            << " service_us="<<service_us<<"\n";

  // spawn worker threads
  std::vector<std::thread> ths; ths.reserve(threads);
  std::vector<std::unique_ptr<ThreadCtx>> ctxs; ctxs.reserve(threads);
  for (int t=0;t<threads;t++) ctxs.emplace_back(new ThreadCtx(t, coros_per_thread, service_us));
  for (int t=0;t<threads;t++) ths.emplace_back([&,t]{ scheduler_loop(*ctxs[t]); });

  // small warmup
  std::this_thread::sleep_for(500ms);

  // start producer
  std::thread prod(producer_rps, rps, duration_s);

  // wait and stop
  prod.join();
  g_running.store(false);
  for (auto& th : ths) th.join();

  auto done = g_done.load();
  std::cout << "Processed="<< done
            << " avg_throughput=" << (done/(double)duration_s) << " req/s"
            << " queue_left=" << g_rx.size() << "\n";
  return 0;
}

