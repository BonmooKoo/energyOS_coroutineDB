// Boost coroutine version of the original cpp coroutine example
//export LANG=ko_KR.UTF-8
// Compile with: 
// g++ -std=c++17 core_consolidation.cpp -o core_consolidation -I./ -I/usr/local/include   -L/usr/local/lib -Wl,-rpath,/usr/local/lib   -lboost_coroutine -lboost_context -lboost_system   -libverbs -lmemcached -lpthread
// 지금은 코루틴을 직접 옮기지만, 이 구현이 너무 무거우면 request만 옮기는 구현이 필요함.

 
#include <boost/coroutine/symmetric_coroutine.hpp>
#include <iostream>
#include <thread>
#include <vector>
#include <queue>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cassert>
#include <sys/syscall.h>
#include <unistd.h>
#include <pthread.h>

constexpr int   MAX_Q  = 64;     // Max Queue length
constexpr double Q_A   = 0.2;    // Queue Down threshold-> Core consolidation
constexpr double Q_B   = 0.9;    // Queue Up threshold -> Load Balancing
constexpr int   MAX_THREADS = 4;
constexpr int   SLO_THRESHOLD_MS=5;
//실험 종료를 알리는 전역변수
std::atomic<bool> g_stop{false};

enum CoreState {SLEEPING, ACTIVE, CONSOLIDATING, STARTED};
enum RequestType {OP_PUT, OP_GET, OP_DELETE, OP_RANGE, OP_UPDATE};

class Scheduler;
using coro_t = boost::coroutines::symmetric_coroutine<Scheduler*>;
using CoroCall = coro_t::call_type; //caller
using CoroYield = coro_t::yield_type; //callee

pid_t gettid() {
    return syscall(SYS_gettid);
}
void bind_cpu(int cpu_num){
    pthread_t this_thread = pthread_self();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_num, &cpuset);
    pthread_setaffinity_np(this_thread, sizeof(cpu_set_t), &cpuset);
}

Scheduler* schedulers[MAX_THREADS] = {nullptr};
std::condition_variable cvs[MAX_THREADS];
std::mutex cv_mutexes[MAX_THREADS];
std::atomic<bool> sleeping_flags[MAX_THREADS];
std::atomic<CoreState> core_state[MAX_THREADS];

// =====================
// Request & MPMC Queue
// =====================
struct Request {
    int     type{0};
    uint64_t key{0};
    uint64_t value{0};
    // 필요하면 타임스탬프, ctx 포인터 등 확장
    uint64_t start_time;
};

// 간단 MPMC 스타일 큐 
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
    //std::lock_guard<std::mutex> lk(m_);
    return q_.size();
  }
  void steal_all(std::deque<Request>& out) {
    std::lock_guard<std::mutex> lk(m_);
    while (!q_.empty()) {
      out.push_back(std::move(q_.front()));
      q_.pop_front();
    }
  }
  void push_bulk(std::deque<Request>& in) {
    if (in.empty()) return;
    {
      std::lock_guard<std::mutex> lk(m_);
      while (!in.empty()) {
        q_.push_back(std::move(in.front()));
        in.pop_front();
      }
    }
    cv_.notify_all();
  }


} g_rx;

// =====================
// Task & Scheduler
// =====================
struct Task {
    CoroCall* source = nullptr; 
    int utask_id{0};
    int thread_id{0};
    int task_type{0};
    uint64_t key{0};
    uint64_t value{0};

    Task() = default;
    Task(CoroCall* src, int tid, int uid, int type=0)
        : source(src), utask_id(uid), thread_id(tid), task_type(type) {}

    Task(Task&& other) noexcept
        : source(other.source), utask_id(other.utask_id),
          thread_id(other.thread_id), task_type(other.task_type),
          key(other.key), value(other.value) {
        other.source = nullptr;
    }

    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            source = other.source;
            utask_id = other.utask_id;
            thread_id = other.thread_id;
            task_type = other.task_type;
            key = other.key; value = other.value;
            other.source = nullptr;
        }
        return *this;
    }

    ~Task() {
        delete source;
    }

    bool is_done() const {
        return source == nullptr || !(*source);
    }

    void resume(Scheduler* sched) {
        if (source && *source) {
            (*source)(sched); // resume coroutine with scheduler pointer
        }
    }
    void set_type(int type){ task_type=type; }
    int get_type() const { return task_type; }
};

class Scheduler {
public:
    int thread_id;
    std::queue<Task> work_queue;     // Work Queue (코루틴 스케줄)
    std::queue<Task> wait_list;      // Wait list (코루틴 대기)
    std::mutex mutex;		    //Mutex for Wait list

    // 스레드 내부 요청 큐 (외부 g_rx에서 옮겨온 Request 소비)
    MPMCQueue rx_queue;

    Scheduler(int tid) : thread_id(tid) {
        schedulers[tid] = this;
        core_state[tid] = ACTIVE;// 시작시 STARTED 단계로 consolidation을 방지.
    }

    bool detect_SLO_violation(){
    	if(rx_queue.size() > Q_B * MAX_Q  ) {
    		return true;
	}
	else return false; 
    }
    bool is_idle() {
    	if(rx_queue.size() < Q_A * MAX_Q  ) {
    		return true;
	}
	else return false; 
    }
    void emplace(Task&& task) {
        //std::lock_guard<std::mutex> lock(mutex);
        work_queue.push(std::move(task));
    }

    void enqueue_to_wait_list(Task&& task) {
        std::lock_guard<std::mutex> lock(mutex);
        wait_list.push(std::move(task));
    }


    bool is_empty() {
        std::lock_guard<std::mutex> lock(mutex);
        return work_queue.empty() && wait_list.empty();
    }

    // 한 번에 wait_list -> work_queue로 옮기고, 한 개 코루틴을 실행
    void schedule() {
        // 1) Wait list -> Work Queue 
        if(!wait_list.empty()){ 
           printf("[%d]pulling wait_list\n",thread_id);
           std::lock_guard<std::mutex> lock(mutex);
           while (!wait_list.empty()) {
                //work_queue.push(std::move(wait_list.front()));
                emplace(std::move(wait_list.front()));
                //printf("[%d]Enqueue<%d>\n",thread_id,wait_list.front().utask_id);
		wait_list.pop();
           }
           printf("[%d]pull fin <%d:%d>\n",thread_id,work_queue.size(),wait_list.size());
	}

        // 2) (네트워크 폴링 자리에 필요 시 추가)

        // 3) Resume one coroutine
	Task task;
	{
        	if (work_queue.empty()) return;
        	task = std::move(work_queue.front());
        	work_queue.pop();
    	}

        // CoroCall에 현재 스케줄러 포인터를 전달
        (*task.source)(this); // `task.resume()` 대신 이렇게 호출

        if (!task.is_done() && !g_stop.load()) {
            emplace(std::move(task));
        }
    }
};

// =====================
// Sleep / Wake helpers
// =====================


void sleep_thread(int tid){
  std::unique_lock<std::mutex> lock(cv_mutexes[tid]);
  sleeping_flags[tid] = true;
  cvs[tid].wait(lock, [&]{ return !sleeping_flags[tid]|| g_stop.load(); });//실험용
  //cvs[tid].wait(lock, [&]{ return !sleeping_flags[tid]; });
}

void wake_up_thread(int tid){
  { std::lock_guard<std::mutex> lk(cv_mutexes[tid]); sleeping_flags[tid] = false; }
  cvs[tid].notify_one();
}
void wake_all_threads(int num_thread) {
    for (int i = 0; i < MAX_THREADS; ++i) {
        { std::lock_guard<std::mutex> lk(cv_mutexes[i]); sleeping_flags[i] = false; }
        cvs[i].notify_all();
    }
}

// ================
//   Migration 처리
// ================
// 오프로딩 시 코루틴 이동 (from -> to)
int post_mycoroutines_to(int from_tid, int to_tid) {
    int count = 0;
    auto& to_sched   = *schedulers[to_tid];
    auto& from_sched = *schedulers[from_tid];

    //std::scoped_lock lk(from_sched.mutex, to_sched.mutex);
    std::lock_guard<std::mutex> lk_to(to_sched.mutex);
    while (!from_sched.work_queue.empty()) {
        count++;
        to_sched.wait_list.push(std::move(from_sched.work_queue.front()));
        from_sched.work_queue.pop();
    }
    printf("[%d:%d]post_coroutineto<%d:%d:%d>\n",from_tid,from_sched.work_queue.size(),to_tid,to_sched.work_queue.size(),to_sched.wait_list.size());
    return count;
}

// 자는 스레드를 깨울 때 코루틴 일부 전달
int load_balancing(int from_tid, int to_tid){
    printf("[%d>>%d]LoadBalancing",from_tid,to_tid);    
    auto& to_sched   = *schedulers[to_tid];
    auto& from_sched = *schedulers[from_tid];

    //std::scoped_lock lk(from_sched.mutex, to_sched.mutex);
    std::lock_guard<std::mutex> lk_to(to_sched.mutex);
    int half = 0;
    // from의 절반 정도만 넘기는 예시 (필요 시 정책 조정)
    {
        std::queue<Task> tmp;
        int total = from_sched.work_queue.size();
        half = total / 2;
        for (int i=0; i<half; ++i) {
            Task t = std::move(from_sched.work_queue.front()); 
            from_sched.work_queue.pop();
            to_sched.wait_list.push(std::move(t));
        }
    }
    return half;
}

// global rx_queue → thread_local rx_queue
static inline void pump_external_requests_into(Scheduler& sched, int burst = 32) {
    Request r;
    int cnt = 0;
    while (cnt < burst && g_rx.try_pop(r)) {
        sched.rx_queue.push(std::move(r));
        cnt++;
    }
}

int sched_load(int c){
    int wq = schedulers[c]->work_queue.size();
    int rx = schedulers[c]->rx_queue.size();
    return wq+rx;
}

int pick_active_random(int self, int also_exclude = -1) {
    // ACTIVE + 유효 스케줄러만
    for (int tries = 0; tries < MAX_THREADS; ++tries) {
        int c = rand() % MAX_THREADS;
        if (c == self || c == also_exclude) continue;
        if (!schedulers[c]) continue;
        if (core_state[c].load() != ACTIVE) continue;
        return c;
    }
    return -1;
}


int power_of_two_choices(int self) {
    int a = pick_active_random(self);
    if (a < 0) return -1;
    int b = pick_active_random(self);
    if (b < 0) return -1;

    size_t la = sched_load(a);
    size_t lb = sched_load(b);
    return (lb > la) ? a : b; // 더 가벼운 쪽(통합 대상으로 좋음)
}

bool state_active_to_consol(int tid){
    CoreState expected = ACTIVE; 
    return core_state[tid].compare_exchange_strong(expected,CONSOLIDATING);
}

int core_consolidation(Scheduler& sched, int tid){
    printf("[%d] 0\n",tid);
    //0)
    if(!state_active_to_consol(tid)){
    	printf("[%d] failed\n",tid);
        return -1; // ME = CONSOLIDATION
    }

    //1)target
    int target = -1;
    //5회 시도
    for (int att = 0; att < 5; ++att) {
        int cand = power_of_two_choices(tid);
        if (cand < 0) break;
        printf("[%d>%d]CONSOLIDATION\n",tid,cand);
        if (state_active_to_consol(cand)) {
            target = cand;
            break;// target = CONSOLIDATION
        }
    }
    if (target < 0) {
        printf("[%d]-1fail\n",tid); 
     // Consolidation Failed
        core_state[tid]=ACTIVE; 
        return -1;
    }
     
    printf("[%d] 2\n",tid);
    //2)move coroutine 
    post_mycoroutines_to(tid,target);	
    //3)move request 
    std::deque<Request> tmp;
    sched.rx_queue.steal_all(tmp); 
    schedulers[target]->rx_queue.push_bulk(tmp);
    //4)change target state
    printf("[%d:%d]ACTIVE\n",tid,target);
    core_state[target]=ACTIVE; 
    return target;
}

// ===============
// Request 처리부
// ===============
static void process_request_on_worker(const Request& r, int tid, int coroid) {
    // 여기에 실제 요청 처리 로직 삽입 (KV, Memcached, RDMA 등)
    // 데모: 간단한 출력 + 약간의 연산/슬립
    /*std::cout << "  [Worker " << tid << "-" << coroid
              << "] handling req type=" << r.type
              << " key=" << r.key << " val=" << r.value
              << " on thread " << gettid() << "\n";
    */
    printf("[Worker%d-%d]Req[%d-%d]\n",tid,coroid,gettid(),r.key);
}

// 워커 코루틴: 깨어날 때마다 rx_queue에서 Request를 소비
// yield_type은 void에서 Scheduler*로 변경
// call_type은 Scheduler*를 받도록 변경

void print_worker(Scheduler& sched, int tid, int coroid) {
    auto* source = new CoroCall([tid, coroid](CoroYield& yield) {
        Scheduler* current = yield.get(); // (*call)(this)로 전달된 포인터
        /*std::cout << "[Coroutine " << tid << "-" << coroid
                 << "] started on thread " << gettid() << "\n";
	*/
        printf("[Coroutine%d-%d]started\n",gettid(),coroid);
        Request r;
        while (true) {
            if (!current->rx_queue.try_pop(r)) {
                yield();
                current = yield.get();
                continue;
            }
            process_request_on_worker(r, tid, coroid);
            yield();
            current = yield.get();
        }
    });

    Task task(source, tid, coroid, 0);
    sched.emplace(std::move(task));
}


void master(Scheduler& sched, int tid, int coro_count) {
    bind_cpu(tid);

    // 워커 코루틴들 생성 (지속적으로 rx_queue에서 요청을 소비)
    for (int i = 0; i < coro_count; ++i) {
        print_worker(sched, tid, tid*coro_count+i);
    }

    int sched_count = 0;
    //while (true) {
    while(!g_stop.load()){//실험을 위해서 g_stop 계속 확인
        // 1) 외부 수신 큐(g_rx) → 스레드 내부 rx_queue로 펌프
        pump_external_requests_into(sched, /*burst*/32);

        // 2) 스케줄링 (코루틴 하나 실행)
        sched.schedule();
        // 2-1) worker 코루틴은 자발적으로 yield를 통해 제어권을 내게 넘겨줌

        // 3) core consolidation : 매 8회마다 스케줄링
        if (++sched_count >= 8) {
            sched_count = 0;
            // 3-1) 저부하면 코어 정리 시도 (core 0는 절대 안꺼짐)
            if (sched.is_idle() && tid != 0) {
                printf("Core[%d] idle\n",tid);
                if (core_consolidation(sched, tid)!=-1) {
    			printf("[%d] sleep\n",tid);
    			core_state[tid]=SLEEPING;
                	if(!g_stop.load()) sleep_thread(tid); // 다른 곳으로 코루틴 넘기고 잠자기
                }
            }
            // 3-2) SLO 위반 시 잠자는 스레드 깨워서 일부 코루틴 이관
            else if (!g_stop.load() && sched.detect_SLO_violation()) {
                for (int i=0;i<MAX_THREADS;i++){
                    if (i!=tid && sleeping_flags[i]) {
                        if(load_balancing(tid,i)!=-1){
                        	wake_up_thread(i);
			}
                        break;
                    }
                }
            }
	}//end if(++sched_count >3)
    }//end while(!g_stop.load())
   
    // (실험용) 외부 큐 빨아오고, 로컬 큐가 빌 때까지 스케줄
    // 실제로는 work coroutine이나 master나 멈추지 않고 계~속 작동하기때문에 이렇게 안함.
    for (;;) {
        pump_external_requests_into(sched, 1024);
        while(sched.rx_queue.size()!=0){
        	sched.schedule();
        }
        break;
    }
    printf("Master ended\n");
}

void thread_func(int tid, int coro_count) {
    bind_cpu(tid);
    Scheduler sched(tid);
    master(sched, tid, coro_count);
    printf("Thread%dfinished\n",tid);
}
void timed_producer(int num_thread,int qps,int durationSec);

int main() {
    const int coro_count   = 10;      // 워커 코루틴 수
    const int num_thread   = 4;      // 워커 스레드 수
    const int durationSec  = 10;     // 실험 시간 (초)
    const int qps          = 50000;  // 초당 요청 개수

    // sleeping_flags 초기화
    for (int i=0;i<MAX_THREADS;i++) sleeping_flags[i] = false;


    // 프로듀서 시작 (T초/QPS)
    std::thread producer(timed_producer, num_thread,qps, durationSec);
    // 워커 시작
    std::thread thread_list[num_thread];
    for (int i = 0; i < num_thread; i++) {
        thread_list[i] = std::thread(thread_func, i, coro_count);
    }

    // 프로듀서 종료 대기 
    producer.join();
    // 잠든 master 깨우기 
    wake_all_threads(num_thread); 
    // 워커 조인
    for (int i = 0; i < num_thread; i++) {
        if (thread_list[i].joinable()) thread_list[i].join();
    }

    return 0;
}



void timed_producer(int num_thread, int qps, int durationSec) {
    bind_cpu(num_thread);

    using clock = std::chrono::steady_clock;
    auto start    = clock::now();
    auto deadline = start + std::chrono::seconds(durationSec);

    // 한 요청 간격
    auto period   = std::chrono::nanoseconds(1'000'000'000LL / std::max(1, qps));
    auto next     = start;

    uint64_t k = 1;
    while (!g_stop.load() && clock::now() < deadline) {
        Request r;
        r.type  = OP_PUT;    // OP_PUT/GET/DELETE/RANGE/UPDATE 중 하나
        r.key   = k++;
        r.value = k * 10;

        // [당신의 현재 g_rx는 mutex 기반이라 실패 반환이 없음]
        g_rx.push(std::move(r));

        // 다음 발사 시각까지 대기 (드리프트 최소화)
        next += period;
        std::this_thread::sleep_until(next);
    }

    // 실험 타임업 → 종료 신호
    g_stop.store(true);
}


