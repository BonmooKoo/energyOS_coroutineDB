#include "rdma_common.h"
#include "rdma_verb.h"
#include "rdma_coroutine.hpp"
#include "keeper.h"
#include <sys/mman.h>

#define MAXTHREAD 32
#define SERVER 1
//#define TIMECHECK

/* These are basic RDMA resources */
struct RdmaContext contexts[MAXTHREAD];
struct ibv_cq *client_cq[MAXTHREAD];
struct ibv_qp *client_qp[MAXTHREAD][SERVER];
/* These are memory buffers related resources */
struct ibv_mr *client_src_mr[MAXTHREAD],*client_dst_mr[MAXTHREAD];

struct serverINFO server_info[SERVER];
ibv_gid server_gid[SERVER];
uint8_t poll_count[MAXTHREAD];
/* Source and Destination buffers, where RDMA operations source and sink */
char* src[MAXTHREAD];
char* dst[MAXTHREAD];

unsigned long readtime[MAXTHREAD];
unsigned long writetime[MAXTHREAD];
unsigned long atomictime[MAXTHREAD];
unsigned long reads[MAXTHREAD];
unsigned long writes[MAXTHREAD];
unsigned long atomics[MAXTHREAD];

unsigned long rdma_readtime(int thread)
{
	return readtime[thread];
}
unsigned long rdma_writetime(int thread)
{
	return writetime[thread];
}
unsigned long rdma_atomictime(int thread)
{
	return atomictime[thread];
}

unsigned long get_reads(int thread){
	return reads[thread];
}
unsigned long get_writes(int thread){
	return writes[thread];
}
unsigned long get_atomics(int thread){
	return atomics[thread];
}

void rdma_settime()
{
	for(int i=0;i<MAXTHREAD;i++){
		readtime[i] = 0;
		writetime[i] = 0;
		atomictime[i]=0;
		reads[i]=0;
		writes[i]=0;
		atomics[i]=0;
	}
}
int check_src_dst()
{
	//printf("src : %s \n ", src);
	//printf("dst : %s \n ", dst);
	return memcmp((void *)src, (void *)dst, 8);
}
char **getdst()
{
	return dst;
}
char **getsrc()
{
	return src;
}
int pollWithCQ(ibv_cq *cq, int pollNumber, struct ibv_wc *wc)
{
	int count = 0;
	do
	{
		int new_count = ibv_poll_cq(cq, 1, wc);
		count += new_count;

	} while (count < pollNumber);

	if (count < 0)
	{
		printf("Poll Completion failed.");
		//exit(1);
		return -1;
	}

	if (wc->status != IBV_WC_SUCCESS)
	{
		printf("Failed status %s (%d) for wr_id %d",ibv_wc_status_str(wc->status), wc->status,(int)wc->wr_id);
		//exit(1);
		return -1;
	}

	return count;
}

int pollOnce(ibv_cq *cq, int pollNumber, struct ibv_wc *wc) {
  int count = ibv_poll_cq(cq, pollNumber, wc);
  if (count <= 0) {
    return 0;
  }
  if (wc->status != IBV_WC_SUCCESS) {
    return -1;
  } else {
    return wc->wr_id;
  }
}

int poll_coroutine(int thread_id) {
    struct ibv_wc wc;
    int ret = ibv_poll_cq(client_cq[thread_id], 1, &wc);

    if (ret == 0) {
        // CQ 비어있음
        return -1;
    }
    if (ret < 0) {
        // CQ 폴링 자체 실패 (리턴값 < 0)
        fprintf(stderr, "poll_coroutine: ibv_poll_cq failed, ret=%d\n", ret);
        return -2;
    }

    // 여기부터는 ret > 0, wc 유효
    if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr,
            "poll_coroutine: QP=0x%x wr_id=%llu status=%s(%d) vend=0x%x len=%u op=%d\n",
            wc.qp_num,
            (unsigned long long)wc.wr_id,
            ibv_wc_status_str(wc.status), wc.status,
            wc.vendor_err, wc.byte_len, wc.opcode);

        // QP가 에러로 가면 남은 WR들이 플러시되어 자주 보이는 케이스
        if (wc.status == IBV_WC_WR_FLUSH_ERR) {
            return -3; // 상위에서 QP 상태 점검/복구
        }
        return -4; // 기타 에러(REM_ACCESS_ERR/LOC_PROT_ERR 등)
    }

    return (int)wc.wr_id;
}

// int client_connection(struct sockaddr_in *s_addr,int server,int thread)
int client_connection(int client, int thread_num, int thread)
{
	//printf("connection start on %d\n", thread + client * thread_num);
	int ret;
	if(!createContext(&contexts[thread],1,3,0)){
		printf("createContext failed\n");
		exit(1);
	}
	client_cq[thread] = ibv_create_cq((contexts[thread].ctx), 128, NULL, NULL, 0);
	if(client_cq[thread]==NULL){
		printf("wrong comp queue gen\n");
		exit(1);
	}
	//printf("%d) cq %p\n",thread,client_cq[thread]);
	uint16_t server_qp_num[2];
	uint32_t server_id[2];
	for(int i=0;i<SERVER;i++){
		client_qp[thread][i]=createQueuePair(&contexts[thread],contexts[thread].pd, client_cq[thread]);	
		if(!changeQueuePairStateToInit(client_qp[thread][i])){
			printf("Queue Pair INIT failed %d\n",errno);
		}
	}
	Keeper memcache;
	if(!memcache.connectMemcached()){
		printf("memcache connect fail\n");
                exit(1);
	}
	//printf("memcache connected\n");
	//printf("Here\n");
	struct RDMA_Data* rdma_attr=(struct RDMA_Data*)malloc(sizeof(struct RDMA_Data));
	//xchange metadata
	//get from server
	for(int i=0;i<SERVER;i++){
		//printf("Get from server %d...\n", i);
		int server_key=1000+500*i+thread+(client*thread_num);// ex client server 2 의 thread 3 = total thread의 11번째 
		char server[4];
		sprintf(server,"%d",server_key);
		size_t size=sizeof(struct RDMA_Data);
		//printf("get cache\n");
                char* attr=memcache.memGet(server,4,&size);
		//printf("get\n");
                memcpy(rdma_attr,attr,sizeof(struct RDMA_Data));
		if(rdma_attr->type1==QP_NUM){
			server_qp_num[i]=rdma_attr->data1;
			server_id[i]=rdma_attr->data2;
		}
		else{
			server_id[i]=rdma_attr->data1;
			server_qp_num[i]=rdma_attr->data2;
			memcpy(&server_gid[i],&rdma_attr->data3,16);
		}
		//printf("Done\n");
		//printf("Server%d : id : %lu qp_num:%lu\n",i,server_id[i],server_qp_num[i]);
	}
	//send to server
	for(int i=0;i<SERVER;i++){
		//printf("Send to server %d...", i);
		uint32_t my_qp_num=getQueuePairNumber(client_qp[thread][i]);
		//send(my_qp_num,my_local_id);
		int client_key=3000+500*i+thread+client*(thread_num);
		char cl_key[4];
		sprintf(cl_key,"%d",client_key);
		//printf("Client key : %d %s\n",client_key,cl_key);
		//printf("Client id : %lu / qp_num: %lu\n",contexts[thread].lid,my_qp_num);
		rdma_attr->type1=LOCAL_ID;
		rdma_attr->data1=contexts[thread].lid;
		rdma_attr->type2=QP_NUM;
		rdma_attr->data2=my_qp_num;
		rdma_attr->type2=GID;
		memcpy(&rdma_attr->data3,&contexts[thread].gid,16);
		memcache.memSet(cl_key,4,(char*)rdma_attr,sizeof(struct RDMA_Data));
		//printf("Done\n");
	}

	for(int i=0;i<SERVER;i++){
		//printf("changeQueuePairStateToRTR %d...", i);
		if(!changeQueuePairStateToRTR(client_qp[thread][i],1,server_qp_num[i],server_id[i],server_gid[i])){
			printf("Queue Pair RTR failed %d\n",errno);
		}
		//printf("Done\n");
	}

	//printf("src malloc...");
	src[thread]=(char*)malloc(SIZEOFNODE);
	
	if(!src[thread]){
		printf("src alloc wrong!\n");
		exit(1);
	}
	//printf("Done\n");

	//printf("dst malloc...");
	dst[thread]=(char*)malloc(SIZEOFNODE);	
	if(!dst[thread]){
		printf("dst alloc wrong!\n");
		exit(1);
	}
	//printf("Done\n");

	client_src_mr[thread]=ibv_reg_mr(contexts[thread].pd, src[thread], SIZEOFNODE, 
										//static_cast<ibv_access_flags>
										(IBV_ACCESS_LOCAL_WRITE |
										  IBV_ACCESS_REMOTE_READ |
										  IBV_ACCESS_REMOTE_WRITE|
										  IBV_ACCESS_REMOTE_ATOMIC));
	if(!client_src_mr[thread]){
		printf("client_src_mr wrong!\n");
		exit(1);
	}
	client_dst_mr[thread]=ibv_reg_mr(contexts[thread].pd, dst[thread], SIZEOFNODE, 
										//static_cast<ibv_access_flags>
										(IBV_ACCESS_LOCAL_WRITE |
										  IBV_ACCESS_REMOTE_READ |
										  IBV_ACCESS_REMOTE_WRITE|
										  IBV_ACCESS_REMOTE_ATOMIC));
	if(!client_dst_mr[thread]){
		printf("client_src_mr wrong!\n");
		exit(1);
	}
	// printf("%d) client_src_mr %lu %p\n",thread,client_src_mr[thread]->lkey,client_src_mr[thread]->addr);
	// printf("%d) client_dst_mr %lu %p\n",thread,client_dst_mr[thread]->lkey,client_dst_mr[thread]->addr);

	//get server lkey & addr
	if(thread==0){
		for(int i=0;i<SERVER;i++){
			memset(rdma_attr,0,sizeof (struct RDMA_Data));
			int server_key=5000+500*i;
			char server[4];
			sprintf(server,"%d",server_key);
			size_t size=sizeof(struct RDMA_Data);
			char* attr=memcache.memGet(server,4,&size);
			memcpy(rdma_attr,attr,sizeof(struct RDMA_Data));
			// printf("Key : %d %s\n",server_key,server);
			server_info[i].address=rdma_attr->data1;
			server_info[i].rkey=rdma_attr->data2;
			// printf("%d> add : %p\n",i,server_info[i].address);
			// printf("%d> rkeys: %lu\n",i,server_info[i].rkey);
		}
	}
	else{
		usleep(1000);
	}
	for(int i=0;i<SERVER;i++){
		if(!changeQueuePairStateToRTS(client_qp[thread][i])){
			printf("Queue Pair RTS failed %d\n",errno);
		}
	}
	return 0;
}

/* This function does :
 * 1) Prepare memory buffers for RDMA operations
 * 1) RDMA write from src -> remote buffer
 * 2) RDMA read from remote bufer -> dst
 */
int rdma_read(uint64_t serveraddress, uint32_t datalength,int server,int thread)
{
#ifdef TIMECHECK
	timespec t1, t2;
	clock_gettime(CLOCK_REALTIME, &t1);
#endif
	struct ibv_sge client_send_sge;
	memset(&client_send_sge,0,sizeof(client_send_sge));
restart_read:
	struct ibv_wc wc;
	int ret = -1;
	// memset(dst,0,SIZEOFNODE);
	// memset(client_dst_mr->addr,0,SIZEOFNODE);
	//client_send_sge.addr = (uint64_t)client_dst_mr[server]->addr;
	client_send_sge.addr = (uint64_t)client_dst_mr[thread]->addr;
	client_send_sge.length = datalength;
	client_send_sge.lkey = client_dst_mr[thread]->lkey;
	/* now we link to the send work request */
	ibv_send_wr client_send_wr;
	ibv_send_wr *bad_client_send_wr;
	bzero(&client_send_wr, sizeof(client_send_wr));
	client_send_wr.sg_list = &client_send_sge;
	client_send_wr.num_sge = 1;
	client_send_wr.opcode = IBV_WR_RDMA_READ;
	client_send_wr.send_flags = IBV_SEND_SIGNALED;
	
	/* we have to tell server side info for RDMA */ 
	client_send_wr.wr.rdma.rkey = server_info[server].rkey;
	client_send_wr.wr.rdma.remote_addr = server_info[server].address+ serveraddress;
	// printf("%d) addr : %p %p\n",thread,client_send_sge.addr,dst[thread]);
	// printf("%d) rkey : %d %lu lkey: %lu\n",thread,server,client_send_wr.wr.rdma.rkey,client_send_sge.lkey);
	// printf("%d) remoteaddr : %p %p\n",thread,client_send_wr.wr.rdma.remote_addr,server_info[server].address);
	// printf("%d) cq : %p\n",thread,client_cq[thread]);
	/* Now we post it */
	ret = ibv_post_send(client_qp[thread][server],
						&client_send_wr,
						&bad_client_send_wr);
	if (ret)
	{
		printf("Failed to read client dst buffer from the master, errno: %d \n",
				   -errno);
		return -errno;
	}

	/* at this point we are expecting 1 work completion for the write */
	ret=pollWithCQ(client_cq[thread], poll_count[thread]+1, &wc);
	poll_count[thread]=0;

	if(ret==-1){
		printf("%d) read poll failed\n",thread);
		exit(1);
		//goto restart_read;
	}
#ifdef TIMECHECK
	clock_gettime(CLOCK_REALTIME, &t2);
	unsigned long timer = (t2.tv_sec - t1.tv_sec) * 1000000000UL + t2.tv_nsec - t1.tv_nsec;
	readtime[thread] += timer;
	reads[thread]++;
	// printf("%d) read_timer %d: %lu %lu\n",thread,server,datalength ,timer);
#endif
	return ret;
}


int rdma_read_nopoll(uint64_t serveraddress, uint32_t datalength,int server,int thread,int coro_id)
{
	struct ibv_sge client_send_sge;
	memset(&client_send_sge,0,sizeof(client_send_sge));
	int ret = -1;
	client_send_sge.addr = (uint64_t)client_dst_mr[thread]->addr;
	client_send_sge.length = datalength;
	client_send_sge.lkey = client_dst_mr[thread]->lkey;
	ibv_send_wr client_send_wr;
	ibv_send_wr *bad_client_send_wr;
	bzero(&client_send_wr, sizeof(client_send_wr));
	client_send_wr.sg_list = &client_send_sge;
	client_send_wr.num_sge = 1;
	client_send_wr.opcode = IBV_WR_RDMA_READ;
	client_send_wr.send_flags = IBV_SEND_SIGNALED;
        //코루틴위해서 추가
	client_send_wr.wr_id=coro_id;
	/* we have to tell server side info for RDMA */ 
	client_send_wr.wr.rdma.rkey = server_info[server].rkey;
	client_send_wr.wr.rdma.remote_addr = server_info[server].address+ serveraddress;
	/* Now we post it */
	ret = ibv_post_send(client_qp[thread][server],
						&client_send_wr,
						&bad_client_send_wr);
	if (ret)
	{
		printf("(rdma_read_nopoll)Worker[%d %d] Failed to read client dst buffer from the master, ret : %d/\ 
 													errno: %d/\ 
													SGE.addr = %p, length = %d, lkey = 0x%x\n",
				thread, coro_id,
				ret,
				-errno,
				client_dst_mr[thread]->addr,
       				client_dst_mr[thread]->length,
       				client_dst_mr[thread]->lkey
					);
		return -errno;
	}

	/*
	        struct ibv_wc wc;
        ret=pollWithCQ(client_cq[thread], poll_count[thread]+1, &wc);
        poll_count[thread]=0;

        if(ret==-1){
                printf("%d) read poll failed\n",thread);
                exit(1);
                //goto restart_read;
        }
	*/
	return ret;
}

int rdma_write(uint64_t clientaddress, uint64_t serveraddress, uint32_t datalength,int server,int thread)
{
	// printf("%d) rdma_write %lu %lu\n",thread,serveraddress,datalength);

#ifdef TIMECHECK
	timespec t1, t2;
	clock_gettime(CLOCK_REALTIME, &t1);
#endif
restart_write:
	struct ibv_wc wc;
	int ret = -1;
	struct ibv_sge client_send_sge;
	memset(&client_send_sge,0,sizeof(client_send_sge));

	memcpy(client_src_mr[thread]->addr, (void *)clientaddress, datalength); // cpy address
	client_send_sge.addr = (uint64_t)client_src_mr[thread]->addr;
	// client_send_sge.addr =clientaddress;
	client_send_sge.length = datalength;
	client_send_sge.lkey = client_src_mr[thread]->lkey;
	/* now we link to the send work request */
	ibv_send_wr client_send_wr;
	ibv_send_wr *bad_client_send_wr;
	bzero(&client_send_wr, sizeof(client_send_wr));
	client_send_wr.sg_list = &client_send_sge;
	client_send_wr.num_sge = 1;
	client_send_wr.opcode = IBV_WR_RDMA_WRITE;
	client_send_wr.send_flags = IBV_SEND_SIGNALED;

	/* we have to tell server side info for RDMA */
	client_send_wr.wr.rdma.rkey = server_info[server].rkey;
	client_send_wr.wr.rdma.remote_addr = (server_info[server].address) + serveraddress;
	// printf("%d) addr : %p %p\n",thread,client_send_sge.addr,src[thread]);
	// printf("%d) rkey : %d %lu lkey %lu\n",thread,server,client_send_wr.wr.rdma.rkey,client_send_sge.lkey);
	// printf("%d) remoteaddr : %p %p\n",thread,client_send_wr.wr.rdma.remote_addr,server_info[server].address);
	// printf("%d) cq : %p\n",thread,client_cq[thread]);

	/* Now we post it */
	ret = ibv_post_send(client_qp[thread][server],
						&client_send_wr,
						&bad_client_send_wr);
	if (ret)
	{
		printf("Failed to write client src buffer, errno: %d \n",
				   -errno);
		return -errno;
	}

	/* at this point we are expecting 1 work completion for the write */
	//poll_count[thread]++;

	ret= pollWithCQ(client_cq[thread], 1, &wc);
	if(ret==-1){
		printf("%d) write poll failed\n",thread);
		exit(1);
		goto restart_write;
	}
#ifdef TIMECHECK
	clock_gettime(CLOCK_REALTIME, &t2);
	unsigned long timer = (t2.tv_sec - t1.tv_sec) * 1000000000UL + t2.tv_nsec - t1.tv_nsec;
	writetime[thread] += timer;
	writes[thread]++;
	//printf("write timer%d :  %lu\n",server,timer);
#endif
	return 0;
}
int rdma_write_batch(uint64_t clientaddress, uint64_t serveraddress, uint32_t datalength,int server,int thread){
#ifdef TIMECHECK
	timespec t1, t2;
	clock_gettime(CLOCK_REALTIME, &t1);
#endif
	// printf("write batch\n");
	struct ibv_wc wc;
	ibv_send_wr client_send_wr[2];
	struct ibv_sge client_send_sge[2];
	ibv_send_wr *bad_client_send_wr;
	memset(&client_send_wr[0],0, sizeof(client_send_wr[0]));
	memset(&client_send_wr[1],0, sizeof(client_send_wr[1]));
	int ret = -1;
	memset(&client_send_sge[0],0,sizeof(client_send_sge[0]));
	memset(&client_send_sge[1],0,sizeof(client_send_sge[1]));

	memcpy(client_src_mr[thread]->addr, (void *)(clientaddress+64), datalength-64); // cpy address
	memcpy(client_dst_mr[thread]->addr, (void *)clientaddress,64);
	//entry first
	client_send_sge[0].addr = (uint64_t)client_src_mr[thread]->addr;
	client_send_sge[0].length = datalength-64;
	client_send_sge[0].lkey = client_src_mr[thread]->lkey;
	client_send_wr[0].sg_list = &client_send_sge[0];
	client_send_wr[0].num_sge = 1;
	client_send_wr[0].opcode = IBV_WR_RDMA_WRITE;
	client_send_wr[0].wr.rdma.rkey = server_info[server].rkey;
	client_send_wr[0].wr.rdma.remote_addr = (server_info[server].address) + serveraddress+64;//Entry 시작주소
	client_send_wr[0].next=&client_send_wr[1];

	//metadata later
	client_send_sge[1].addr = (uint64_t)client_dst_mr[thread]->addr;
	client_send_sge[1].length = 64;
	client_send_sge[1].lkey = client_dst_mr[thread]->lkey;
	client_send_wr[1].sg_list = &client_send_sge[1];
	client_send_wr[1].num_sge = 1;
	client_send_wr[1].opcode = IBV_WR_RDMA_WRITE;
	client_send_wr[1].send_flags = IBV_SEND_SIGNALED;
	client_send_wr[1].wr.rdma.rkey = server_info[server].rkey;
	client_send_wr[1].wr.rdma.remote_addr = (server_info[server].address) + serveraddress;//Metadata 시작주소
	client_send_wr[1].next=NULL;

	/* Now we post it */
	ret = ibv_post_send(client_qp[thread][server],
						&client_send_wr[0],
						&bad_client_send_wr);
	if (ret!=0)
	{
		printf("Failed to write client src buffer, errno: %d %d \n",
				   -errno,ret);
		return -errno;
	}
	int pollret=0;
	pollret=pollWithCQ(client_cq[thread], 1, &wc);
#ifdef TIMECHECK
	clock_gettime(CLOCK_REALTIME, &t2);
	unsigned long timer = (t2.tv_sec - t1.tv_sec) * 1000000000UL + t2.tv_nsec - t1.tv_nsec;
	writetime[thread] += timer;
	writes[thread]++;
	//printf("batch write timer%d :  %lu\n",server,timer);
#endif
	if(pollret==-1){
		printf("%d) write batch poll failed\n",thread);
	}
	// rdma_read(serveraddress,datalength,server,thread);
	// reinterpret_cast<Node*>(dst[thread])->checkNode();
	// free(&client_send_sge[0]);
	// free(&client_send_sge[1]);
	// free(&client_send_wr[0]);
	// free(&client_send_wr[1]);

	// printf("end\n");
	return 0;
}

int rdma_CAS(uint64_t compare, uint64_t swap, uint64_t serveraddress, uint32_t datalength,int server,int thread)
{
#ifdef TIMECHECK
	timespec t1, t2;
	clock_gettime(CLOCK_REALTIME, &t1);
#endif
	struct ibv_sge client_send_sge;
	memset(&client_send_sge,0,sizeof(client_send_sge));

	struct ibv_wc wc;
	int ret = -1;
	memset(client_src_mr[thread]->addr,0,1024);
	client_send_sge.addr = (uint64_t)client_src_mr[thread]->addr;
	client_send_sge.length = datalength;
	client_send_sge.lkey = client_src_mr[thread]->lkey;
	/* now we link to the send work request */
	ibv_send_wr client_send_wr;
	ibv_send_wr *bad_client_send_wr;
	bzero(&client_send_wr, sizeof(client_send_wr));
	client_send_wr.sg_list = &client_send_sge;
	client_send_wr.num_sge = 1;
	client_send_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
	client_send_wr.send_flags = IBV_SEND_SIGNALED;

	/* we have to tell server side info for RDMA */
	client_send_wr.wr.atomic.rkey = server_info[server].rkey;
	client_send_wr.wr.atomic.remote_addr = (server_info[server].address) + serveraddress;
	client_send_wr.wr.atomic.compare_add = compare;
	client_send_wr.wr.atomic.swap = swap;
	//printf("%d) addr : %p %p\n",thread,client_send_sge.addr,src[thread]);
	//printf("%d) rkey : %d %lu %lu\n",thread,server,client_send_wr.wr.atomic.rkey,client_send_sge.lkey);
	//printf("%d) remoteaddr : %p %p\n",thread,client_send_wr.wr.atomic.remote_addr,server_info[server].address);
	//printf("%d) cq : %p\n",thread,client_cq[thread]);

	/* Now we post it */
	ret = ibv_post_send(client_qp[thread][server],
						&client_send_wr,
						&bad_client_send_wr);
	//printf("%d) post_send : %p\n",thread,client_qp[thread][server]);
	if (ret)
	{
		printf("Send with ATOMIC CAS failed\n");
		return false;
	}
	ret = pollWithCQ(client_cq[thread],poll_count[thread]+1, &wc);
	poll_count[thread]=0;

	if(ret == -1){
		printf("%d) CAS poll failed\n",thread);
		exit(1);	
	}
	uint64_t compare_swap=__builtin_bswap64(compare);
#ifdef TIMECHECK
	clock_gettime(CLOCK_REALTIME, &t2);
	unsigned long timer = (t2.tv_sec - t1.tv_sec) * 1000000000UL + t2.tv_nsec - t1.tv_nsec;
	atomictime[thread]+=timer;
	atomics[thread]++;
	//printf("CAS time %d: %lu ",server, timer);
#endif
	//printf("%d)compare\n",thread);
	if(memcmp((void*)client_src_mr[thread]->addr,&compare,8)==0){
	//if(compare==*(uint64_t*)client_src_mr[thread]->addr){
		return true;
	}
	else{
		return false;
	}
}

int rdma_CAS_returnvalue(uint64_t compare, uint64_t swap, uint64_t serveraddress, uint32_t datalength,int server,int thread)
{
#ifdef TIMECHECK
	timespec t1, t2;
	clock_gettime(CLOCK_REALTIME, &t1);
#endif
	struct ibv_sge client_send_sge;
	memset(&client_send_sge,0,sizeof(client_send_sge));

	struct ibv_wc wc;
	int ret = -1;
	memset(client_src_mr[thread]->addr,0,1024);
	client_send_sge.addr = (uint64_t)client_src_mr[thread]->addr;
	client_send_sge.length = datalength;
	client_send_sge.lkey = client_src_mr[thread]->lkey;
	/* now we link to the send work request */
	ibv_send_wr client_send_wr;
	ibv_send_wr *bad_client_send_wr;
	bzero(&client_send_wr, sizeof(client_send_wr));
	client_send_wr.sg_list = &client_send_sge;
	client_send_wr.num_sge = 1;
	client_send_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
	client_send_wr.send_flags = IBV_SEND_SIGNALED;

	/* we have to tell server side info for RDMA */
	client_send_wr.wr.atomic.rkey = server_info[server].rkey;
	client_send_wr.wr.atomic.remote_addr = (server_info[server].address) + serveraddress;
	client_send_wr.wr.atomic.compare_add = compare;
	client_send_wr.wr.atomic.swap = swap;
	// printf("%d) addr : %p %p\n",thread,client_send_sge.addr,src[thread]);
	// printf("%d) rkey : %d %lu %lu\n",thread,server,client_send_wr.wr.atomic.rkey,client_send_sge.lkey);
	// printf("%d) remoteaddr : %p %p\n",thread,client_send_wr.wr.atomic.remote_addr,server_info[server].address);
	// printf("%d) cq : %p\n",thread,client_cq[thread]);
	
	/* Now we post it */
	ret = ibv_post_send(client_qp[thread][server],
						&client_send_wr,
						&bad_client_send_wr);
	if (ret)
	{
		printf("Send with ATOMIC CAS failed\n");
		return false;
	}
	ret = pollWithCQ(client_cq[thread],poll_count[thread]+1, &wc);
	poll_count[thread]=0;

	if(ret == -1){
		printf("%d) CAS poll failed\n",thread);
		exit(1);	
	}
	uint64_t compare_swap=__builtin_bswap64(compare);
#ifdef TIMECHECK
	clock_gettime(CLOCK_REALTIME, &t2);
	unsigned long timer = (t2.tv_sec - t1.tv_sec) * 1000000000UL + t2.tv_nsec - t1.tv_nsec;
	atomictime[thread]+=timer;
	atomics[thread]++;
	//printf("CAS time %d: %lu ",server, timer);
#endif
	return *(uint64_t*)(client_src_mr[thread]->addr);
}


int rdma_FAA(uint64_t* clientaddress, uint64_t add, uint64_t serveraddress, uint32_t datalength,int server,int thread)
{
#ifdef TIMECHECK
	timespec t1, t2;
	clock_gettime(CLOCK_REALTIME, &t1);
#endif
	struct ibv_sge client_send_sge;
	memset(&client_send_sge,0,sizeof(client_send_sge));
	struct ibv_wc wc;
	int ret = -1;
	memset(client_src_mr[thread]->addr,0,1024);
	client_send_sge.addr = (uint64_t)client_src_mr[thread]->addr;
	client_send_sge.length = datalength;
	client_send_sge.lkey = client_src_mr[thread]->lkey;
	
	/* now we link to the send work request */
	ibv_send_wr client_send_wr;
	ibv_send_wr *bad_client_send_wr;
	bzero(&client_send_wr, sizeof(client_send_wr));
	client_send_wr.sg_list = &client_send_sge;
	client_send_wr.num_sge = 1;
	client_send_wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
	client_send_wr.send_flags = IBV_SEND_SIGNALED;
	
	/* we have to tell server side info for RDMA */
	client_send_wr.wr.atomic.rkey = server_info[server].rkey;
	client_send_wr.wr.atomic.remote_addr = server_info[server].address + serveraddress;
	client_send_wr.wr.atomic.compare_add = add;
	// printf("rdma_write : target addr %p\n",server_metadata_attr[server].address+serveraddress);

	/* Now we post it */
	ret = ibv_post_send(client_qp[thread][server],
						&client_send_wr,
						&bad_client_send_wr);
	if (ret)
	{
		printf("Send with ATOMIC FAA failed\n");
		return false;
	}

	ret=pollWithCQ(client_cq[thread], poll_count[thread]+1, &wc);
	poll_count[thread]=0;
	if(ret == -1){
		printf("%d) FAA poll failed\n",thread);
		exit(1);	
	}
	//uint64_t compare_swap=__builtin_bswap64(*(uint64_t*)client_src_mr[thread]->addr);
	//printf("FAA: %p %lu %p\n",compare_swap,compare_swap,*(uint64_t*)client_src_mr[thread]->addr);
	
	//memcpy(clientaddress,&compare_swap,8);
	memcpy(clientaddress,client_src_mr[thread]->addr,8);
#ifdef TIMECHECK
	clock_gettime(CLOCK_REALTIME, &t2);
	unsigned long timer = (t2.tv_sec - t1.tv_sec) * 1000000000UL + t2.tv_nsec - t1.tv_nsec;
	atomictime[thread]+=timer;
	atomics[thread]++;
	//printf("FAA time : %lu\n", timer);

#endif
	return true;
}

int client_disconnect_and_clean(int threadcount)
{
	for(int i=0;i<threadcount;i++){
		//printf("clean QP\n");
		ibv_destroy_qp(client_qp[i][0]);
		//printf("clean CQ\n");
		ibv_destroy_cq(client_cq[i]);
		//printf("clean MR\n");
		ibv_dereg_mr(client_src_mr[i]);
		//printf("clean DST\n"); 
		free(src[i]);
		free(dst[i]);
	}

	return 0;
}
