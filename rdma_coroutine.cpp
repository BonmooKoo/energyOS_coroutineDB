#include "rdma_coroutine.hpp"

//coroutine
thread_local CoroCall master;
thread_local int thread_id;
thread_local static int* key=nullptr;
thread_local static uint64_t g_total_ops = 0;
thread_local static std::atomic<uint64_t> g_ops_started{0};
thread_local static std::atomic<uint64_t> g_ops_finished{0};


static int get_key(){
   uint64_t idx = g_ops_started.fetch_add(1, std::memory_order_relaxed);
   //printf("idx : %d\n",idx);
   idx=idx%g_total_ops;//g_total_ops = TOTALOP = 32M
   return key[g_total_ops*thread_id+idx];
}
// 2) Worker 코루틴 본체
static void coro_worker(CoroYield &yield,
                        int coro_id)
{
  int key;
  while(g_ops_started < g_total_ops){
   //1) get key
    key=get_key();
    // 2) RDMA post (pseudo code)
//    printf("Worker[%d] : post read\n",coro_id);
    rdma_read_nopoll((key % (ALLOCSIZE / SIZEOFNODE)) * SIZEOFNODE,8, 0,thread_id,coro_id);
//    int ret = rdma_read_nopoll((key % (ALLOCSIZE / SIZEOFNODE)) * SIZEOFNODE,8, 0,thread_id,coro_id);
//    printf("Worker[%d] : read [ %d ]\n" , coro_id,ret);
    // 3) 완료 대기: master 로 제어권 넘기기
    yield(master);
//    printf("Worker[%d] : read fin\n",coro_id);
   // (resume 후) 후처리
    ++g_ops_finished;
  }

  // 루프 종료 후에도 master에게 복귀
  yield(master);
}

// 3) Master 코루틴 본체
static void coro_master(CoroYield &yield,
                        int coro_cnt,
                        std::vector<CoroCall> &worker)
{
  // 3-1) 초기 실행: 각 워커를 한 번 깨워서 시작하게 만듦
  for (int i = 0; i < coro_cnt; ++i) {
 //   	printf("Master : started Worker Coroutine %d\n",i);
	yield(worker[i]);
  }

  // 3-2) 이벤트 루프: CQ 폴링l
  while (g_ops_finished < g_total_ops) {
  //  printf("Master :try to poll Job\n");
    int next_id=poll_coroutine(thread_id); // ret : -1 : failed / 0~ : coro_id
    if(next_id<0){
	continue;
    }
    else{
    //printf("Master :polled Job for coro_id %d \n",next_id);
    yield(worker[next_id]);
    }
  }
}
// run_coroutine(int thread_id,int coro_cnt, int* key[],int threadcount)
// 4) run_coroutine 함수: Tree::run_coroutine 과 동일한 형태
void run_coroutine(int tid,
                          int coro_cnt,
                          int* key_arr,
                          int threadcount,
                          int total_ops
                        )
{
  //bind thread
  pthread_t this_thread = pthread_self();
  thread_id = tid;
    // CPU 집합 만들기
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(thread_id, &cpuset);
  int ret = pthread_setaffinity_np(this_thread, sizeof(cpu_set_t), &cpuset);
    if (ret != 0) {
        perror("pthread_setaffinity_np");
    }

  //0.key
  key=key_arr;
  g_total_ops=total_ops;
  //1. coroutine vector 생성
  std::vector<CoroCall> worker(coro_cnt);
 
  //2. Client 생성
  for (int i = 0; i < coro_cnt; ++i) {
    worker[i] = CoroCall(std::bind(&coro_worker,/*yield*/ _1,/*coro_id*/ i));
  }

  // 4-2) 마스터 코루틴 생성
  master = CoroCall(
    std::bind(&coro_master, _1, coro_cnt, std::ref(worker))
  );
  // 4-3) 최초 진입
  master();
}
