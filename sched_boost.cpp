// Boost coroutine version of the original cpp coroutine example
// Compile with: 
// g++ -std=c++17 sched_boost.cpp -o sched_boost -I./ -I/usr/local/include   -L/usr/local/lib -Wl,-rpath,/usr/local/lib   -lboost_coroutine -lboost_context -lboost_system   -libverbs -lmemcached -lpthread

#include <random>
#include <chrono>
#include <boost/coroutine/symmetric_coroutine.hpp>
#include <iostream>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <cassert>
#include <sys/syscall.h>
#include <unistd.h>
#include <pthread.h>

pid_t gettid() {
    return syscall(SYS_gettid);
}

constexpr int MAX_THREADS = 32;

class Scheduler;
Scheduler* schedulers[MAX_THREADS] = {nullptr};

using coro_t    = boost::coroutines::symmetric_coroutine<void>;
using CoroCall = coro_t::call_type;   // 주 체험자
using CoroYield = coro_t::yield_type; // 협력자

struct Task {
    CoroCall* source = nullptr;  // 또는 CoroYield* depending on role
    int utask_id;
    int thread_id;

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
};

class Scheduler {
public:
    int thread_id;
    std::queue<Task> coroutine_queue;
    std::queue<Task> wait_list;
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

int post_mycoroutines_to(int from_tid, int to_tid) {
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

void worker(Scheduler& sched, int tid, int coroid) {
    auto* source = new CoroCall([=](CoroYield& yield) {
        std::cout << "[Coroutine " << tid << "-" << coroid << "] started on thread " << gettid() << "\n";
        yield();
        std::cout << "[Coroutine " << tid << "-" << coroid << "] ended on thread " << gettid() << "\n";
    });
    sched.emplace(Task(source, tid, coroid));
}


void master(Scheduler& sched, int tid, int coro_count) {
    for (int i = 0; i < coro_count; ++i) {
        worker(sched, tid, tid * 100 + i);
    }
    int count = 0;
    while (count++ < 3) {
        sched.schedule();
    }
    if (tid == 1) {
        post_mycoroutines_to(1, 0);
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

    while (true) {
        sched.schedule();
        if (sched.coroutine_queue.empty() && sched.wait_list.empty()) break;
    }

    std::cout << "Thread " << tid << " finished.\n";
}

int main() {
    const int coro_count = 10;

    std::thread t0(thread_func, 0, coro_count);
    std::thread t1(thread_func, 1, coro_count);

    t1.join();
    t0.join();
    return 0;
}
