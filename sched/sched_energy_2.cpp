// 2sched_energy.cpp
// Build:
/*
 g++ -std=gnu++17 -O2 2sched_energy.cpp -o 2sched_energy \
   -I/opt/boost-1.60/include -L/opt/boost-1.60/lib \
   -Wl,-rpath,/opt/boost-1.60/lib \
   -lboost_coroutine -lboost_context -lboost_system -lpthread

taskset -c 0 ./2sched_energy --mode thread --threads 1 --rps 20000 --duration 60 --service-us 200 --quantum-us 200
taskset -c 0 ./2sched_energy --mode thread --threads 1 --rps 20000 --duration 60 --service-us 200 --quantum-us 200

*/
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
#include <condition_variable>
#include <unistd.h>
#include <sys/syscall.h>
#include <pthread.h>

using namespace std::chrono;

// ----------------- Globals -----------------
static std::atomic<bool> g_running{true};
static std::atomic<uint64_t> g_req_id{0};
static std::atomic<uint64_t> g_done{0};

struct Request { uint64_t id; uint64_t ts_ns; };

class MPMCQueue {
  mutable std::mutex m_;
  std::condition_variable cv_;
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
    {
      std::lock_guard<std::mutex> lk(m_);
      q_.push_back(std::move(r));
    }
    cv_.notify_one();
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

static inline void busy_for_us(int us){
  auto t0 = steady_clock::now();
  while (duration_cast<microseconds>(steady_clock::now()-t0).count() < us) {
    cpu_relax();
  }
}
static inline void pin_to_cpu(int cpu){
  cpu_set_t set; CPU_ZERO(&set); CPU_SET(cpu, &set);
  (void)pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

// ----------------- Coroutine scheduler -----------------
struct ThreadCtx {
  int cpu_id;
  int coros;
  int service_us;
  int quantum_us;
  std::vector<std::unique_ptr<Call>> workers;
  explicit ThreadCtx(int cpu, int c, int sus, int qus)
    : cpu_id(cpu), coros(c), service_us(sus), quantum_us(qus) {}
};

static thread_local bool tl_did_work = false;

void make_coro_worker(ThreadCtx& ctx){
  auto fn = [&](Yield& y){
    Request req{};
    while (g_running.load(std::memory_order_relaxed)) {
      if (g_rx.try_pop(req)) {
        tl_did_work = true;
        // 서비스 시간을 작은 퀀텀으로 쪼개고 매 퀀텀마다 유저레벨 전환
        int remain = ctx.service_us;
        while (remain > 0) {
          int q = std::min(ctx.quantum_us, remain);
          busy_for_us(q);
          remain -= q;
          y(); // 유저레벨 컨텍스트 스위치
        }
        g_done.fetch_add(1, std::memory_order_relaxed);
      } else {
        tl_did_work = false;
        y(); // 일이 없을 때도 코퍼레이티브 양보
      }
    }
  };
  ctx.workers.emplace_back(new Call(fn));
}

void coroutine_scheduler_loop(ThreadCtx& ctx){
  pin_to_cpu(ctx.cpu_id);
  for (int i=0;i<ctx.coros;i++) make_coro_worker(ctx);

  size_t n = ctx.workers.size(), cursor = 0;
  const int RUN_BATCH_MAX = 64;

  while (g_running.load(std::memory_order_relaxed)) {
    for (int r=0; r<RUN_BATCH_MAX && n>0; ++r) {
      auto& w = ctx.workers[cursor];
      cursor = (cursor + 1) % n;
      if (*w) (*w)();
    }
    std::this_thread::yield();
  }
}

// ----------------- Thread workers -----------------
struct ThreadWorkerCfg {
  int cpu_id;
  int service_us;
  int quantum_us;
};

void thread_worker(ThreadWorkerCfg cfg){
  pin_to_cpu(cfg.cpu_id);
  Request req{};
  while (g_running.load(std::memory_order_relaxed)) {
    if (!g_rx.try_pop(req)) {
      // 항상 runnable 상태를 유지(스케줄러가 돌릴 후보) - 과도한 스핀 방지로 약간 양보
      std::this_thread::yield();
      continue;
    }
    // 서비스 시간을 퀀텀으로 쪼개고 매 퀀텀마다 커널에 양보 → 커널 컨텍스트 스위치 유도
    int remain = cfg.service_us;
    while (remain > 0) {
      int q = std::min(cfg.quantum_us, remain);
      busy_for_us(q);
      remain -= q;
      std::this_thread::yield(); // 커널 스케줄러 양보 (CS 증가)
    }
    g_done.fetch_add(1, std::memory_order_relaxed);
  }
}

// ----------------- Producer -----------------
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
    else next = now;
  }
}

// ----------------- Main -----------------
int main(int argc, char** argv){
  enum class Mode { Coroutine, Thread };
  Mode mode = Mode::Coroutine;

  // 입력 파라미터
  int active_cores = 4;        // k: 16/4/1
  int total_workers = 64;      // 항상 64 고정
  int rps=1000, duration_s=30;
  int service_us=200;
  int quantum_us=50;           // 매 전환 간 퀀텀 (전환 빈도 조절)

  // 간단 파서: --mode {thread|coroutine}, --threads k, --rps, --duration, --service-us, --quantum-us
  auto arg_eq = [](const char* s, const char* k)->const char*{
    size_t n=strlen(k); return (!strncmp(s,k,n) && s[n]=='=') ? s+n+1 : nullptr;
  };
  for (int i=1;i<argc;i++){
    if (!strcmp(argv[i],"--mode") && i+1<argc){ mode = (!strcmp(argv[++i],"thread")?Mode::Thread:Mode::Coroutine); }
    else if (auto v=arg_eq(argv[i],"--mode")) { mode = (!strcmp(v,"thread")?Mode::Thread:Mode::Coroutine); }
    else if (!strcmp(argv[i],"--threads") && i+1<argc){ active_cores=atoi(argv[++i]); }
    else if (auto v=arg_eq(argv[i],"--threads")) { active_cores=atoi(v); }
    else if (!strcmp(argv[i],"--rps") && i+1<argc){ rps=atoi(argv[++i]); }
    else if (auto v=arg_eq(argv[i],"--rps")) { rps=atoi(v); }
    else if (!strcmp(argv[i],"--duration") && i+1<argc){ duration_s=atoi(argv[++i]); }
    else if (auto v=arg_eq(argv[i],"--duration")) { duration_s=atoi(v); }
    else if (!strcmp(argv[i],"--service-us") && i+1<argc){ service_us=atoi(argv[++i]); }
    else if (auto v=arg_eq(argv[i],"--service-us")) { service_us=atoi(v); }
    else if (!strcmp(argv[i],"--quantum-us") && i+1<argc){ quantum_us=atoi(argv[++i]); }
    else if (auto v=arg_eq(argv[i],"--quantum-us")) { quantum_us=atoi(v); }
  }

  // 64 workers를 k 코어에 고르게 분배
  int workers_per_core = total_workers / active_cores; // 가정: 64가 k로 나눠떨어짐(16,4,1은 OK)
  if (total_workers % active_cores != 0) {
    std::cerr << "total_workers(64) must be divisible by active_cores\n";
    return 1;
  }

  std::cout << "mode="<<(mode==Mode::Thread?"thread":"coroutine")
            << " active_cores="<<active_cores
            << " total_workers=64"
            << " per_core="<<workers_per_core
            << " rps="<<rps
            << " duration="<<duration_s<<"s"
            << " service_us="<<service_us
            << " quantum_us="<<quantum_us
            << "\n";

  std::vector<std::thread> ths;

  if (mode == Mode::Coroutine) {
    // k개의 워커 스레드(=코어) 생성, 각 스레드당 64/k 코루틴
    for (int c=0; c<active_cores; ++c) {
      auto* ctx = new ThreadCtx(/*cpu*/c, /*coros*/workers_per_core, service_us, quantum_us);
      ths.emplace_back([ctx]{
        coroutine_scheduler_loop(*ctx);
        delete ctx;
      });
    }

    std::this_thread::sleep_for(500ms);
    std::thread prod(producer_rps, rps, duration_s); prod.join();
    g_running.store(false);
    for (auto& th: ths) th.join();

  } else {
    // 64개의 OS 스레드를 만들고, k개 코어에 균등 핀(코어 c에 per_core개)
    ths.reserve(total_workers);
    int created = 0;
    for (int c=0; c<active_cores; ++c) {
      for (int i=0; i<workers_per_core; ++i) {
        ThreadWorkerCfg cfg{ .cpu_id=c, .service_us=service_us, .quantum_us=quantum_us };
        ths.emplace_back(thread_worker, cfg);
        ++created;
      }
    }
    // 안전: 분배 안 맞을 때 남은 것은 라운드로빈
    for (; created<total_workers; ++created) {
      ThreadWorkerCfg cfg{ .cpu_id=created % active_cores, .service_us=service_us, .quantum_us=quantum_us };
      ths.emplace_back(thread_worker, cfg);
    }

    std::this_thread::sleep_for(500ms);
    std::thread prod(producer_rps, rps, duration_s); prod.join();
    g_running.store(false);
    // 깨우기용 더미 몇 개 밀어넣어 종료 보장
    for (int i=0;i<total_workers;i++) g_rx.push(Request{UINT64_MAX,0});
    for (auto& th: ths) th.join();
  }

  auto done = g_done.load();
  std::cout << "Processed="<< done
            << " avg_throughput=" << (done/(double)duration_s) << " req/s"
            << " queue_left=" << g_rx.size() << "\n";
  return 0;
}

