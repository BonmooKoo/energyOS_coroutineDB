#include "rdma_verb.h"
#include "rdma_common.h"
#include <boost/coroutine/symmetric_coroutine.hpp>
#include <vector>
#include <functional>
#include <atomic>
#include <infiniband/verbs.h>
// 1) Boost.Coroutine2 타입 정의
using coro_t    = boost::coroutines::symmetric_coroutine<void>;
using CoroCall  = coro_t::call_type;
using CoroYield = coro_t::yield_type;
using namespace std::placeholders;

void run_coroutine(int thread_id,
                          int coro_cnt,
                          int* key_arr,
                          int threadcount,
                          int total_ops
                        );
