// Boost coroutine version of the original cpp coroutine example
// Compile with: 
// g++ -std=c++17 sched_workqueue.cpp -o sched_workqueue -I./ -I/usr/local/include   -L/usr/local/lib -Wl,-rpath,/usr/local/lib   -lboost_coroutine -lboost_context -lboost_system   -libverbs -lmemcached -lpthread

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

using coro_t    = boost::coroutines::symmetric_coroutine<void>;
using CoroCall = coro_t::call_type;   // 코루틴을 호출하거나 재개하는 객체(Caller) : 코루틴 객체를 가지고 있는 쪽
using CoroYield = coro_t::yield_type; // 코루틴 내부에서 호출자에게 제어를 넘기는 객체(Callee)

pid_t gettid() {
    return syscall(SYS_gettid);
}

constexpr int MAX_THREADS = 4;
constexpr int SLO_THRESHOLD_MS=5;
bool detect_SLO_violation(int tid) {
    // Placeholder: insert real latency measurement logic
    return rand() % 100 < 10; // 10% chance to violate
}

class Scheduler;
Scheduler* schedulers[MAX_THREADS] = {nullptr};
std::condition_variable cvs[MAX_THREADS];
std::mutex cv_mutexes[MAX_THREADS];
std::atomic<bool> sleeping_flags[MAX_THREADS];

struct Task {
    CoroCall* source = nullptr;  // 또는 CoroYield* depending on role
    int utask_id;
    int thread_id;
    int task_type;
    uint64_t key;
    uint64_t value;
    Task(CoroCall* src, int tid, int uid, int type)
        : source(src), utask_id(uid), thread_id(tid),task_type(type) {}
    Task(CoroCall* src, int tid, int uid)
        : source(src), utask_id(uid), thread_id(tid) {}

    Task(Task&& other) noexcept
        : source(other.source), utask_id(other.utask_id), thread_id(other.thread_id) {
        other.source = nullptr;
    }

    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            source = other.source;
            utask_id = other.utask_id;
            thread_id = other.thread_id;
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

    void resume() {
        if (source && *source) {
            (*source)();  // 코루틴을 재개
        }
    }
    void set_type(int type){
	task_type=type;
    }
    int get_type(){
	return task_type;	
    }
};

class Scheduler {
public:
    int thread_id;
    std::queue<Task> coroutine_queue; // 실행중 / 실행 예정인 coroutine이 대기하는 queue
    std::queue<Task> wait_list;//thread(나포함,다른 thread포함)가 wait_list에서 Task를 가져와서 그걸 coroutine_queue에 넣는다.
    std::mutex mutex;

    Scheduler(int tid) : thread_id(tid) {
        schedulers[tid] = this;
    }

    void emplace(Task&& task) {
        coroutine_queue.push(std::move(task));
    }

    void enqueue_to_wait_list(Task&& task) {
        std::lock_guard<std::mutex> lock(mutex);
        wait_list.push(std::move(task));
    }
    bool is_idle() {
        std::lock_guard<std::mutex> lock(mutex);
        return coroutine_queue.size() <= 3;
    }
    bool is_empty() {
        std::lock_guard<std::mutex> lock(mutex);
        return coroutine_queue.empty() && wait_list.empty();
    }

    void schedule() {
        {
            std::lock_guard<std::mutex> lock(mutex);
            while (!wait_list.empty()) {
                coroutine_queue.push(std::move(wait_list.front()));
                wait_list.pop();
            }
        }

        if (!coroutine_queue.empty()) {
            Task task = std::move(coroutine_queue.front());
            coroutine_queue.pop();

            task.resume();

            if (!task.is_done()) {
                emplace(std::move(task));
            }
        }
    }
};

//Sleep&& wakeup thread using Conditional Variable
void sleep_thread(int tid){
  std::unique_lock<std::mutex> lock(cv_mutexes[tid]);
  sleeping_flags[tid] = true;
  //wait until someone (other master coroutine) change my sleeping_flag
  cvs[tid].wait(lock, [&]{ return !sleeping_flags[tid]; });
}

void wake_up_thread(int tid){
  { std::lock_guard<std::mutex> lk(cv_mutexes[tid]); sleeping_flags[tid] = false; }
  cvs[tid].notify_one();
}

//When thread sleep
int post_mycoroutines_to(int from_tid, int to_tid) {
    int count = 0;
    auto& to_sched = *schedulers[to_tid];
    auto& from_sched = *schedulers[from_tid];
    std::lock_guard<std::mutex> lock_to(to_sched.mutex);
    
    while (!from_sched.coroutine_queue.empty()) {
        count++;
        to_sched.wait_list.push(std::move(from_sched.coroutine_queue.front()));
        from_sched.coroutine_queue.pop();
    }
    return count;
}
//When thread awake
int post_coroutine_to_awake(int from_tid,int to_tid){
    int count = 0;
    auto& to_sched = *schedulers[to_tid];
    auto& from_sched = *schedulers[from_tid];
    std::lock_guard<std::mutex> lock_from(from_sched.mutex);
    std::lock_guard<std::mutex> lock_to(to_sched.mutex);
    while (!from_sched.coroutine_queue.empty()) {
        count++;
        to_sched.wait_list.push(std::move(from_sched.coroutine_queue.front()));
        from_sched.coroutine_queue.pop();
    }
    return count;
}

//Make worker coroutine and emplace @ WorkQueue
void print_worker(Scheduler& sched, int tid, int coroid) {
    auto* source = new CoroCall([=](CoroYield& yield) {
        std::cout << "[Coroutine " << tid << "-" << coroid << "] started on thread " << gettid() << "\n";
        yield();
        std::cout << "[Coroutine " << tid << "-" << coroid << "] ended on thread " << gettid() << "\n";
    });
    sched.emplace(Task(source, tid, coroid));
}

void read_worker(Scheduler& sched, int tid, int coroid) {
    auto* source = new CoroCall([=](CoroYield& yield) {
        std::cout << "[Coroutine " << tid << "-" << coroid << "] started on thread " << gettid() << "\n";
        yield();
        std::cout << "[Coroutine " << tid << "-" << coroid << "] ended on thread " << gettid() << "\n";
    });
    sched.emplace(Task(source, tid, coroid));
}

void write_worker(Scheduler& sched, int tid, int coroid) {
    auto* source = new CoroCall([=](CoroYield& yield) {
        std::cout << "[Coroutine " << tid << "-" << coroid << "] started on thread " << gettid() << "\n";
        yield();
        std::cout << "[Coroutine " << tid << "-" << coroid << "] ended on thread " << gettid() << "\n";
    });
    sched.emplace(Task(source, tid, coroid));
}

int num_thread;
bool try_offload_coroutine(Scheduler& sched, int tid){
    //power of two 기반, 랜덤하게 2개를 선택해서 그중 더 적은 부분에 넣으면 나름 equal하게 load balancing 이 가능함.
    constexpr int MAX_ATTEMPTS = 5;//시도 횟수 5회시도
    int attempts = 0;
    int target;
    while (attempts++ < MAX_ATTEMPTS) {
        int i = rand() % MAX_THREADS;
        int j = rand() % MAX_THREADS;

        while (i == tid || schedulers[i] == nullptr) {
            i = (i + 1) % MAX_THREADS;
        }
        while (j == tid || j == i || schedulers[j] == nullptr) {
            j = (j + 2) % MAX_THREADS;
        }

        target = -1;

        if (!schedulers[i]->is_idle()) {
            target = i;
        }
        if (!schedulers[j]->is_idle()) {
            if (target==-1 || schedulers[j]->coroutine_queue.size() > schedulers[i]->coroutine_queue.size()) {
                target = j;
            }
        }

        if (target!=-1) {
            post_mycoroutines_to(tid, target);
            if (sched.is_empty()) {
                schedulers[tid] = nullptr;
                return true;  // 오프로드 성공
            }
        }
    }
    return false;  // 오프로드 실패
}
void master(Scheduler& sched, int tid, int coro_count) {
    pthread_t this_thread = pthread_self();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(tid, &cpuset);
    pthread_setaffinity_np(this_thread, sizeof(cpu_set_t), &cpuset);    
    //create coroutine (init)
    for (int i = 0; i < coro_count; ++i) {
        print_worker(sched, tid, tid * 100 + i);
    }

    // Scheduling
    int sched_count = 0;
    while (true) {
	//poll network request
		

	//start next thread
        sched.schedule();
	if(sched_count++>3){
	    sched_count=0;
	    if (sched.is_idle() && tid != 0) {
		if (try_offload_coroutine(sched, tid)) {
	        	sleep_thread(tid);
	        }
    	    }
	}
        if(detect_SLO_violation(tid)){
		for(int i=0;i<MAX_THREADS;i++){
			if(i!=tid && sleeping_flags[i]){
				//awake up sleeping thread and hand over my half of coroutine
				post_coroutine_to_awake(tid,i);
				wake_up_thread(i);
				break;
			}
		}
        }
    }
}
void thread_func(int tid, int coro_count) {
    pthread_t this_thread = pthread_self();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(tid, &cpuset);
    pthread_setaffinity_np(this_thread, sizeof(cpu_set_t), &cpuset);

    Scheduler sched(tid);
    master(sched, tid, coro_count);
     
    std::cout << "Thread " << tid << " finished.\n";
}

int main() {
    const int coro_count = 10;
    num_thread=4;
    std::thread thread_list[num_thread];
    for(int i=0;i<num_thread;i++){
	    thread_list[i] = std::thread(thread_func, i, coro_count);
    }
    
    for (int i = 0; i < num_thread; i++) {
        thread_list[i].join();
    }
    return 0;
}
