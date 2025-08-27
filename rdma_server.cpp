#include "rdma_common.h"
#include "keeper.h"
/*
순서
1. 서버가 자기 local id랑 qp_num을 써
2. Client가 그걸 읽고 그 밑에 local id랑 qp_num을 써
3. 서버는 그걸 읽고 queue pair을 RTR로 바꿔
4. 서버가 자기의 mem reg을 써
5. Client가 그걸 읽어
*/
#define MAXCLIENT 32
char* dst;
struct ibv_qp* queue_pair[MAXCLIENT];
struct ibv_cq* completion_queue;
struct ibv_mr* mem_region;
uint16_t client_id[MAXCLIENT];
uint32_t client_qp_num[MAXCLIENT];
ibv_gid client_gid[MAXCLIENT];

#include <signal.h>

void cleanup_rdma() {
    for (int i = 0; i < MAXCLIENT; i++) {
        ibv_destroy_qp(queue_pair[i]);
    }
    ibv_destroy_cq(completion_queue);
    ibv_dereg_mr(mem_region);
    munmap(dst, ALLOCSIZE);
    printf("[INFO] 자원 정리 완료\n");
    exit(0);
}

void sigint_handler(int sig) {
    printf("\n[INFO] Ctrl+C 감지. 자원 정리 중...\n");
    cleanup_rdma();
}


int main(int argc,char* argv[]){
	int ret;
	char* server_name;
	int client_num=atoi(argv[1]);
	int server_num=0;
	server_name="server0";
	signal(SIGINT, sigint_handler);
	RdmaContext context;
	createContext(&context,1,3,0);

	completion_queue = ibv_create_cq(context.ctx,128, nullptr, nullptr, 0);

	for(int i=0;i<client_num;i++){
		queue_pair[i]=createQueuePair(&context,context.pd, completion_queue);	
		if(!changeQueuePairStateToInit(queue_pair[i])){
			printf("Queue Pair INIT failed %d\n",errno);
		}
	}

	//Send My local id & qp _ num
	printf("memcache ON\n");
	Keeper memcache;
	memcache.connectMemcached();
	//printf("memcache end\n");
	struct RDMA_Data* rdma_attr=(struct RDMA_Data*)malloc(sizeof(struct RDMA_Data));
	for(int i=0;i<client_num;i++){
		int server_key=1000+500*server_num+i;
		char server[4];
		sprintf(server,"%d",server_key);
		printf("Server: Write %d %s\n",server_key,server);
		size_t size=sizeof(struct RDMA_Data);
		uint32_t my_qp_num=getQueuePairNumber(queue_pair[i]);
		//struct RDMA_Data rdma_attr(LOCAL_ID,my_local_id,QP_NUM, my_qp_num);
		rdma_attr->type1=LOCAL_ID;
		rdma_attr->data1=context.lid;
		rdma_attr->type2=QP_NUM;
		rdma_attr->data2=my_qp_num;
		rdma_attr->type3=GID;
		memcpy(&rdma_attr->data3,&context.gid,16);
		memcache.memSet(server,4,(char*)rdma_attr,sizeof(struct RDMA_Data));
		printf("Server%d : id : %lu qp_num:%lu\n",i,context.lid,my_qp_num);
	}
	printf("waiting for Client Data\n");
	//Read Client qp num /dest_id
	for(int i=0;i<client_num;i++){
		int client_key=3000+500*server_num+i;
		char client[4];
		size_t size=sizeof(struct RDMA_Data);
		sprintf(client,"%d",client_key);
		//printf("client_key : %d %s\n",client_key,client);
		char* attr=memcache.memGet(client,4,&size);
		memcpy(rdma_attr,attr,sizeof(struct RDMA_Data));
		if(rdma_attr->type1==LOCAL_ID){
			client_id[i]=rdma_attr->data1;
			client_qp_num[i]=rdma_attr->data2;
			memcpy(&client_gid[i],&rdma_attr->data3,16);
		}
		else{
			client_id[i]=rdma_attr->data2;
			client_qp_num[i]=rdma_attr->data1;
		}
		printf("Client%d : id : %lu qp_num:%lu\n",i,client_id[i],client_qp_num[i]);
	}
	//
	for(int i=0;i<client_num;i++){
		changeQueuePairStateToRTR(queue_pair[i],1,client_qp_num[i],client_id[i],client_gid[i]);
	}
	printf("Init end\n");

	dst=alloc_dst();
	//dst = (char*)malloc(ALLOCSIZE);
	if(!dst){
		printf("dst mem null\n");
		exit(1);
	}	//posix_memalign((void**)&dst,1024,ALLOCSIZE);

	size_t allocsize=(uint64_t)ALLOCSIZE;
	mem_region=ibv_reg_mr(context.pd, dst, allocsize, 
										static_cast<ibv_access_flags>
										(IBV_ACCESS_LOCAL_WRITE |
										  IBV_ACCESS_REMOTE_READ |
										  IBV_ACCESS_REMOTE_WRITE|
										  IBV_ACCESS_REMOTE_ATOMIC));
	if(!mem_region){
		printf("%d erno\n",errno);
	}

	//init end
	//write_memregion info
	//한번만 적어도 됨
	for(int i=0;i<client_num;i++){
		changeQueuePairStateToRTS(queue_pair[i]);
	}
	int server_key=5000+500*server_num;
	char server[4];
	sprintf(server,"%d",server_key);
	printf("Server_key : %d\n",server_key);
	size_t size=sizeof(struct RDMA_Data);
	memset(rdma_attr,0,sizeof(struct RDMA_Data));
	rdma_attr->type1=ADDR;
	rdma_attr->type2=LKEY;
	rdma_attr->data1= (uint64_t)mem_region->addr;
	rdma_attr->data2= (uint64_t)mem_region->rkey;
	memcache.memSet(server,4,(char*)rdma_attr,sizeof(struct RDMA_Data));
	printf("Write Server Dst Addr\n %p %d\n",mem_region->addr,mem_region->rkey);
	int enter;
	std::cin>>enter;
	printf("clean QP\n");
	for(int i=0;i<client_num;i++){
		ibv_destroy_qp(queue_pair[i]);
	}
	printf("clean CQ\n");
	ibv_destroy_cq(completion_queue);
	printf("clean MR\n");
	ibv_dereg_mr(mem_region);
	printf("clean DST\n");
	munmap(dst,ALLOCSIZE);
	return 0;
}
