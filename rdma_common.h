#ifndef RDMA_COMMON_H
#define RDMA_COMMON_H

#include <iostream>
#include <cstring>
#include <cstdio>
#include <stdio.h>
#include <cerrno>
#include <getopt.h>
#include <sys/mman.h>

#include <netdb.h>
#include <netinet/in.h>	
#include <arpa/inet.h>
#include <sys/socket.h>

#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

#define ALLOCSIZE 1*1024*1024*1024 //64GB 
#define SIZEOFNODE 1024 
#define LOCAL_ID 1
#define QP_NUM 2
#define ADDR 3
#define LKEY 4
#define GID 5
struct RdmaContext {
  uint8_t devIndex;
  uint8_t port;
  int gidIndex;

  ibv_context *ctx;
  ibv_pd *pd;

  uint16_t lid;
  union ibv_gid gid;

  RdmaContext() : ctx(NULL), pd(NULL) {}
};

struct RDMA_Data {
    uint8_t type1;
    uint8_t type2;
    uint8_t type3;
    uint64_t data1;
    uint64_t data2;
    char data3[16];
};

bool createContext(RdmaContext *context, uint8_t port, int gidIndex,
                   uint8_t devIndex);
char* alloc_dst();
//struct ibv_qp* createQueuePair(struct ibv_pd* pd, struct ibv_cq* cq);
struct ibv_qp* createQueuePair(RdmaContext *context,struct ibv_pd* pd, struct ibv_cq* cq);

bool changeQueuePairStateToInit(struct ibv_qp* queue_pair);
bool changeQueuePairStateToRTR(struct ibv_qp* queue_pair, int ib_port, uint32_t destination_qp_number, uint16_t destination_local_id,ibv_gid remoteGid);
//bool changeQueuePairStateToRTR(struct ibv_qp* queue_pair, int ib_port, uint32_t destination_qp_number, uint16_t destination_local_id);
bool changeQueuePairStateToRTS(struct ibv_qp* queue_pair);
uint16_t getLocalId(struct ibv_context* context, int ib_port);
uint32_t getQueuePairNumber(struct ibv_qp* qp);

#endif
