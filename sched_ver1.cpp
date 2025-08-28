// sched_workqueue.cpp
// Build:
// g++ -std=gnu++17 -O2 sched_workqueue.cpp -o sched_workqueue \
//     -lboost_coroutine -lboost_context -lboost_system -lpthread

#include <boost/coroutine/symmetric_coroutine.hpp>
#include <iostream>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cassert>
#include <sys/syscall.h>
#include <unistd.h>
#include <pthread.h>

using namespace std::chrono;

constexpr int   MAX_THREADS            = 4;
constexpr int   SLO_THRESHOLD_MS       = 5;   // demo stub에서만 사용

// === 튜닝 상수 ===
constexpr int   WAIT_DRAIN_MAX         = 64;  // 한 번에 wait->work로 옮기는 최대 개수
constexpr int   RUN_BATCH_MAX          = 16;  // 한 schedule에서 연속 resume 최대 개수
constexpr int   SPIN_YIELD_ROUNDS      = 200; // 바쁜-폴링 억제: 초반 스핀/양보 횟수
constexpr int   IDLE_SLEEP_US          = 50;  // 완전 유휴시 마이크로 슬립
constexpr int   SLEEP_HYSTERESIS_LOOPS = 10000; // 슬립 전 최소 유휴 루프 수
constexpr int   SLEEP_HYSTERESIS_MS    = 2;   // 슬립 전 최소 유휴 시간
constexpr size_t MIGRATION_MIN_BACKLOG = 8;   // 이 이상일 때만 마이그레이션 시도
constexpr size_t MIGRATION_MIN_MOVE    = 4;   // 이동 최소 개수(가능하면)

// === 타입/유틸 ===
using coro_t    = boost::coroutines::symmetric_coroutine<void>;
using CoroCall  = coro_t::call_type;
using CoroYield = coro_t::yield_type;

enum CoreState { ACTIVE, CONSOLIDATING, SLEEPING };

pid_t gettid() { return static_cast<pid_t>(syscall(SYS_gettid)); }

// ---- 전역(데모용) ----
class Scheduler;
static Scheduler* schedulers[MAX_THREADS] = {nullptr};
static std::condition_variable cvs[MAX_THREADS];
static std::mutex cv_mutexes[MAX_THREADS];
static std::atomic<bool> sleeping_flags[MAX_THREADS];
static std::atomic<bool> g_running{true};
static int num_thread = 2;

// SLO 위반 감지(데모: 10% 확률)
bool detect_SLO_violation(int /*tid*/) {
  return (std::rand() % 100) < 10;
}

// ---------------- Task ----------------
struct Task {
  CoroCall* source = nullptr;
  int utask_id = -1;
  int thread_id = -1;
  int task_type = 0;
  uint64_t key = 0;
  uint64_t value = 0;

  Task() = default;
  Task(CoroCall* src, int tid, int uid, int type = 0)
      : source(src), utask_id(uid), thread_id(tid), task_type(type) {}

  Task(Task&& other) noexcept
      : source(other.source),
        utask_id(other.utask_id),
        thread_id(other.thread_id),
        task_type(other.task_type),
        key(other.key),
        value(other.value) {
    other.source = nullptr;
  }
  Task& operator=(Task&& other) noexcept {
    if (this != &other) {
      cleanup();
      source    = other.source;
      utask_id  = other.utask_id;
      thread_id = other.thread_id;
      task_type = other.task_type;
      key       = other.key;
      value     = other.value;
      other.source = nullptr;
    }
    return *this;
  }
  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;

  ~Task() { cleanup(); }

  void cleanup() {
    if (source) { delete source; source = nullptr; }
  }

  bool is_done() const { return (source == nullptr) || !(*source); }

  void resume() {
    if (source && *source) { (*source)(); }
  }

  void set_type(int type) { task_type = type; }
  int  get_type() const { return task_type; }
};

// -------------- Scheduler --------------
class Scheduler {
public:
  int thread_id;
  // 스케줄러 전용: 마스터만 접근(락 X)
  std::queue<Task> work_queue;
  // 교차 스레드 이동은 여기로(락 O)
  std::queue<Task> wait_list;
  std::mutex       mutex;

  explicit Scheduler(int tid) : thread_id(tid) {
    schedulers[tid] = this;
  }

  void emplace(Task&& task) { work_queue.push(std::move(task)); }

  void enqueue_to_wait_list(Task&& task) {
    std::lock_guard<std::mutex> lock(mutex);
    wait_list.push(std::move(task));
  }

  // 대략적 backlog
  size_t backlog_nolock() const { return work_queue.size() + wait_list.size(); }

  size_t backlog() {
    std::lock_guard<std::mutex> lock(mutex);
    return work_queue.size() + wait_list.size();
  }

  bool is_idle() {
    std::lock_guard<std::mutex> lock(mutex);
    return (work_queue.size() + wait_list.size()) <= 3;
  }

  bool is_empty() {
    std::lock_guard<std::mutex> lock(mutex);
    return work_queue.empty() && wait_list.empty();
  }

  // 한 스텝 스케줄: 작업 처리 여부 반환
  bool schedule_once() {
    bool did_work = false;

    // 1) wait_list -> work_queue (배치 드레인)
    {
      std::lock_guard<std::mutex> lock(mutex);
      int drained = 0;
      while (!wait_list.empty() && drained < WAIT_DRAIN_MAX) {
        work_queue.push(std::move(wait_list.front()));
        wait_list.pop();
        ++drained;
      }
      if (drained > 0) did_work = true;
    }

    // 2) (여기서 RDMA CQ poll 처리 가능) // TODO: poll_network()

    // 3) RUN_BATCH_MAX 만큼 연속 실행(라운드로빈)
    int ran = 0;
    while (!work_queue.empty() && ran < RUN_BATCH_MAX) {
      Task task = std::move(work_queue.front());
      work_queue.pop();

      task.resume();
      did_work = true;

      if (!task.is_done()) {
        // 재적재
        work_queue.push(std::move(task));
      }
      ++ran;
    }

    // 4) 완전 유휴면 살짝 양보
    if (!did_work) std::this_thread::yield();
    return did_work;
  }
};

// ---------- Sleep/Wake ----------
void sleep_thread(int tid) {
  std::unique_lock<std::mutex> lock(cv_mutexes[tid]);
  sleeping_flags[tid].store(true, std::memory_order_relaxed);
  cvs[tid].wait(lock, [&]{
    return !g_running.load(std::memory_order_relaxed) ||
           !sleeping_flags[tid].load(std::memory_order_relaxed);
  });
}
void wake_up_thread(int tid) {
  { std::lock_guard<std::mutex> lk(cv_mutexes[tid]);
    sleeping_flags[tid].store(false, std::memory_order_relaxed);
  }
  cvs[tid].notify_one();
}

// from_tid의 wait_list 일부를 to_tid로 이동
int move_waitlist_some(int from_tid, int to_tid, size_t want) {
  if (schedulers[from_tid] == nullptr || schedulers[to_tid] == nullptr) return 0;
  auto& to_sched   = *schedulers[to_tid];
  auto& from_sched = *schedulers[from_tid];

  std::scoped_lock lk(from_sched.mutex, to_sched.mutex);

  int count = 0;
  size_t have = from_sched.wait_list.size();
  size_t take = std::min(want, have);

  for (size_t i = 0; i < take; ++i) {
    to_sched.wait_list.push(std::move(from_sched.wait_list.front()));
    from_sched.wait_list.pop();
    ++count;
  }
  return count;
}

// 절반(또는 최소치) 이동
int move_waitlist_half(int from_tid, int to_tid) {
  if (schedulers[from_tid] == nullptr || schedulers[to_tid] == nullptr) return 0;
  auto& from_sched = *schedulers[from_tid];

  std::lock_guard<std::mutex> lk(from_sched.mutex);
  size_t sz   = from_sched.wait_list.size();
  size_t half = sz / 2;
  size_t want = half;
  if (want < MIGRATION_MIN_MOVE && sz >= MIGRATION_MIN_MOVE) want = MIGRATION_MIN_MOVE;
  // 잠금 해제 후 실제 이동(락 순서 보장 위해 함수 재사용)
  // (move_waitlist_some에서 다시 락을 잡지만 순서가 다중락이라 안전하게 처리)
  return move_waitlist_some(from_tid, to_tid, want);
}

// 랜덤 파워오브투 선택 → 덜 바쁜 스레드로 오프로딩 (임계치 만족 시)
bool try_offload_coroutine(Scheduler& sched, int tid) {
  constexpr int MAX_ATTEMPTS = 5;
  for (int a = 0; a < MAX_ATTEMPTS; ++a) {
    auto pick_next = [&](int seed, int avoid) {
      for (int k = 0; k < MAX_THREADS; ++k) {
        int t = (seed + k) % MAX_THREADS;
        if (t != avoid && schedulers[t] != nullptr) return t;
      }
      return avoid;
    };
    int i = pick_next(std::rand() % MAX_THREADS, tid);
    int j = pick_next((std::rand() + 1) % MAX_THREADS, tid);
    if (i == tid || j == tid || i == j) continue;

    size_t my_bg = sched.backlog();
    if (my_bg < MIGRATION_MIN_BACKLOG) return false;

    // 덜 바쁜 쪽
    size_t bi = schedulers[i]->backlog();
    size_t bj = schedulers[j]->backlog();
    int target = (bi <= bj) ? i : j;

    int moved = move_waitlist_half(tid, target);
    (void)moved;

    if (sched.is_empty()) {
      // 코어 0은 항상 깨어있게 유지
      if (tid == 0) return false;
      schedulers[tid] = nullptr; // 데모용 표시
      return true;
    }
  }
  return false;
}

// 워커 코루틴 생성
void make_worker(Scheduler& sched, int tid, int coroid, int steps = 5) {
  auto* source = new CoroCall([=](CoroYield& y) {
    std::cout << "[Worker " << tid << "-" << coroid << "] start on TID " << gettid() << "\n";
    for (int i = 0; i < steps; ++i) {
      // 네트워크 I/O 발행 자리 (post_send/recv)
      y(); // I/O 동안 양보
      std::cout << "[Worker " << tid << "-" << coroid << "] step " << i << " on TID " << gettid() << "\n";
    }
    std::cout << "[Worker " << tid << "-" << coroid << "] done on TID " << gettid() << "\n";
  });
  sched.emplace(Task(source, tid, coroid));
}

// 마스터 루프(스케줄러)
void master(Scheduler& sched, int tid, int coro_count) {
  // 초기 워커 적재
  for (int i = 0; i < coro_count; ++i) {
    make_worker(sched, tid, tid * 100 + i, 3 + (i % 3));
  }

  size_t idle_spins = 0;
  auto   last_active = steady_clock::now();

  while (g_running.load(std::memory_order_relaxed)) {
    bool did_work = sched.schedule_once();

    if (did_work) {
      idle_spins = 0;
      last_active = steady_clock::now();
    } else {
      // 적응적 백오프
      if (idle_spins < SPIN_YIELD_ROUNDS) {
        ++idle_spins;
        std::this_thread::yield();
      } else {
        std::this_thread::sleep_for(std::chrono::microseconds(IDLE_SLEEP_US));
      }
    }

    // 오프로딩/슬립(히스테리시스)
    auto idle_ms = duration_cast<milliseconds>(steady_clock::now() - last_active).count();
    if (idle_spins > SLEEP_HYSTERESIS_LOOPS && idle_ms >= SLEEP_HYSTERESIS_MS && tid != 0) {
      if (sched.is_idle()) {
        if (try_offload_coroutine(sched, tid)) {
          // 슬립 전 프리필: 깨어날 때 받을 대상에게 일부 넘겨놓고 자진다(이미 move_waitlist_half 내장)
          sleep_thread(tid);
          idle_spins = 0;
          last_active = steady_clock::now();
        }
      }
    }

    // 지연 SLO 위반(데모): 자는 스레드 깨우고 일부 작업 미리 이관(프리필) 후 웨이크
    if (detect_SLO_violation(tid)) {
      for (int i = 0; i < num_thread; ++i) {
        if (i != tid && sleeping_flags[i].load(std::memory_order_relaxed)) {
          // 프리필: 넘어갈 스레드에 내 대기 일부를 선행 이관
          move_waitlist_half(tid, i);
          wake_up_thread(i);
          break;
        }
      }
    }
  }
}

void thread_func(int tid, int coro_count) {
  // CPU affinity (코어 핀)
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(tid % std::thread::hardware_concurrency(), &cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

  Scheduler sched(tid);
  master(sched, tid, coro_count);

  // 종료 시 슬립 상태면 깨워서 빠져나오게
  wake_up_thread(tid);
  std::cout << "Thread " << tid << " exit.\n";
}

int main() {
  std::srand(static_cast<unsigned>(time(nullptr)));
  // 슬립 플래그 초기화
  for (int i = 0; i < MAX_THREADS; ++i) sleeping_flags[i].store(false);

  const int coro_count = 32;
  num_thread = 2; // 데모: 2개 스레드

  std::vector<std::thread> ths;
  ths.reserve(num_thread);
  for (int i = 0; i < num_thread; ++i) {
    ths.emplace_back(thread_func, i, coro_count);
  }

  // 데모 러닝 타임
  std::this_thread::sleep_for(std::chrono::seconds(3));
  g_running.store(false);
  // 자는 스레드들 깨우기
  for (int i = 0; i < num_thread; ++i) wake_up_thread(i);

  for (auto& t : ths) t.join();
  std::cout << "Done.\n";
  return 0;
}

