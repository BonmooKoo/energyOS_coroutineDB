#ifndef RDMA_VERB_H
#define RDMA_VERB_H

#include <iostream>
#include <cstring>
#include <cerrno>
#include <getopt.h>
#include <netdb.h>
#include <netinet/in.h>	
#include <arpa/inet.h>
#include <sys/socket.h>
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <time.h>

#define ALLOCSIZE 1*1024*1024*1024
struct serverINFO{
    uint64_t address;
    uint32_t rkey;
    char padd[4];
};
char** getdst();
int check_src_dst();
int client_connection(int client, int thread_num, int thread);

int pollWithCQ(ibv_cq *cq, int pollNumber, struct ibv_wc *wc);
int pollOnce(ibv_cq *cq, int pollNumber, struct ibv_wc *wc);
int poll_coroutine(int thread_id);


int rdma_read(uint64_t serveraddress,uint32_t datalength,int server,int thread);
int rdma_read_nopoll(uint64_t serveraddress,uint32_t datalength,int server,int thread,int id);
int rdma_write(uint64_t clientaddress,uint64_t serveraddress,uint32_t datalength,int server,int thread);
int rdma_write_batch(uint64_t clientaddress, uint64_t serveraddress, uint32_t datalength,int server,int thread);

int rdma_CAS(uint64_t compare,uint64_t swap,uint64_t serveraddress,uint32_t datalength,int server,int thread);
int rdma_CAS_returnvalue(uint64_t compare,uint64_t swap,uint64_t serveraddress,uint32_t datalength,int server,int thread);
int rdma_FAA(uint64_t* clientaddress,uint64_t add, uint64_t serveraddress,uint32_t datalength,int server,int thread);

int client_disconnect_and_clean(int threadcount);

unsigned long rdma_readtime(int thread);
unsigned long rdma_writetime(int thread);
unsigned long rdma_atomictime(int thread);
unsigned long get_reads(int thread);
unsigned long get_writes(int thread);
unsigned long get_atomics(int thread); 
void rdma_settime();
void set_thread(int thread_id);
#endif
