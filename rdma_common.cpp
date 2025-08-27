#include "rdma_common.h"

bool createContext(RdmaContext *context, uint8_t port, int gidIndex,
                   uint8_t devIndex) {

  ibv_device *dev = NULL;
  ibv_context *ctx = NULL;
  ibv_pd *pd = NULL;
  ibv_port_attr portAttr;

  // get device names in the system
  int devicesNum;
  struct ibv_device **deviceList = ibv_get_device_list(&devicesNum);
  if (!deviceList) {
    goto CreateResourcesExit;
  }

  // if there isn't any IB device in host
  if (!devicesNum) {
    goto CreateResourcesExit;
  }
  // Debug::notifyInfo("Open IB Device");

  for (int i = 0; i < devicesNum; ++i) {
    // printf("Device %d: %s\n", i, ibv_get_device_name(deviceList[i]));
    if (ibv_get_device_name(deviceList[i])[5] == '0') {
      devIndex = i;
      break;
    }
  }

  if (devIndex >= devicesNum) {
    goto CreateResourcesExit;
  }

  dev = deviceList[devIndex];
  // printf("I open %s :)\n", ibv_get_device_name(dev));

  // get device handle
  ctx = ibv_open_device(dev);
  if (!ctx) {
    goto CreateResourcesExit;
  }
  struct ibv_device_attr dev_attr;
  ibv_query_device(ctx,&dev_attr);
  //printf("max_mr_size= %llu\n",dev_attr.max_mr_size);
  /* We are now done with device list, free it */
  ibv_free_device_list(deviceList);
  deviceList = NULL;

  // query port properties
  if (ibv_query_port(ctx, port, &portAttr)) {
    goto CreateResourcesExit;
  }

  // allocate Protection Domain
  // Debug::notifyInfo("Allocate Protection Domain");
  pd = ibv_alloc_pd(ctx);
  if (!pd) {
    goto CreateResourcesExit;
  }

  if (ibv_query_gid(ctx, port, gidIndex, &context->gid)) {
    goto CreateResourcesExit;
  }

  // Success :)
  context->devIndex = devIndex;
  context->gidIndex = gidIndex;
  context->port = port;
  context->ctx = ctx;
  context->pd = pd;
  context->lid = portAttr.lid;

  return true;

/* Error encountered, cleanup */
CreateResourcesExit:
  printf("Error Encountered, Cleanup ...\n");

  if (pd) {
    ibv_dealloc_pd(pd);
    pd = NULL;
  }
  if (ctx) {
    ibv_close_device(ctx);
    ctx = NULL;
  }
  if (deviceList) {
    ibv_free_device_list(deviceList);
    deviceList = NULL;
  }

  return false;
}

char* alloc_dst(){
  	size_t allocsize=(uint64_t)ALLOCSIZE;
  	char* dst=(char*)mmap(NULL, allocsize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
	//char* dst = (char*)malloc(ALLOCSIZE);
	for(uint64_t i=(uint64_t)dst;i<(uint64_t)dst+allocsize;i+=1024){
		*(char*)i=0;
	}
	return dst;
}

struct ibv_qp* createQueuePair(RdmaContext *context,struct ibv_pd* pd, struct ibv_cq* cq) {
  // struct ibv_qp_init_attr queue_pair_init_attr;
  // memset(&queue_pair_init_attr, 0, sizeof(queue_pair_init_attr));
  // queue_pair_init_attr.qp_type = IBV_QPT_RC;
  // queue_pair_init_attr.sq_sig_all = 1;       // if not set 0, all work requests submitted to SQ will always generate a Work Completion.
  // queue_pair_init_attr.send_cq = cq;         // completion queue can be shared or you can use distinct completion queues.
  // queue_pair_init_attr.recv_cq = cq;         // completion queue can be shared or you can use distinct completion queues.
  // queue_pair_init_attr.cap.max_send_wr = 1;  // increase if you want to keep more send work requests in the SQ.
  // queue_pair_init_attr.cap.max_recv_wr = 1;  // increase if you want to keep more receive work requests in the RQ.
  // queue_pair_init_attr.cap.max_send_sge = 1; // increase if you allow send work requests to have multiple scatter gather entry (SGE).
  // queue_pair_init_attr.cap.max_recv_sge = 1; // increase if you allow receive work requests to have multiple scatter gather entry (SGE).

  // return ibv_create_qp(pd, &queue_pair_init_attr);
  struct ibv_qp_init_attr_ex attr;
  memset(&attr, 0, sizeof(attr));

  attr.qp_type = IBV_QPT_RC;
  attr.sq_sig_all = 0;
  attr.send_cq = cq;
  attr.recv_cq = cq;
  attr.pd = context->pd;
  attr.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS |IBV_QP_INIT_ATTR_PD;
  attr.cap.max_send_wr = 32;
  attr.cap.max_recv_wr = 1;
  attr.cap.max_send_sge = 32;
  attr.cap.max_recv_sge = 1;
//  attr.cap.max_inline_data = 256;

  ibv_qp *qp = ibv_create_qp_ex(context->ctx, &attr);
  //ibv_qp *qp = ibv_create_qp(context->ctx, &attr);
  if (qp==NULL) {
    printf("Failed to create QP\n");
    return NULL;
  }

  // Debug::notifyInfo("Create Queue Pair with Num = %d", (*qp)->qp_num);

  return qp;
}

bool changeQueuePairStateToInit(struct ibv_qp* queue_pair) {
  struct ibv_qp_attr init_attr;
  memset(&init_attr, 0, sizeof(init_attr));
  init_attr.qp_state = ibv_qp_state::IBV_QPS_INIT;
  init_attr.port_num = 1;
  init_attr.pkey_index = 0;
  init_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
  if(ibv_modify_qp(queue_pair, &init_attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS) == 0){
    return true;
  }else{
    printf("Failed Init\n");
    return false;
  }
}

bool changeQueuePairStateToRTR(struct ibv_qp* queue_pair, int ib_port, uint32_t destination_qp_number, uint16_t destination_local_id,ibv_gid remoteGid) {
//bool changeQueuePairStateToRTR(struct ibv_qp* queue_pair, int ib_port, uint32_t destination_qp_number, uint16_t destination_local_id){
  struct ibv_qp_attr rtr_attr;
  memset(&rtr_attr, 0, sizeof(rtr_attr));
  rtr_attr.qp_state = ibv_qp_state::IBV_QPS_RTR;
  rtr_attr.path_mtu = ibv_mtu::IBV_MTU_4096;
  rtr_attr.rq_psn = 1217;
  rtr_attr.max_dest_rd_atomic = 16;
  rtr_attr.min_rnr_timer = 12;

  rtr_attr.dest_qp_num = destination_qp_number;

  rtr_attr.ah_attr.dlid = destination_local_id;
  rtr_attr.ah_attr.sl = 0;
  rtr_attr.ah_attr.src_path_bits = 0;
  rtr_attr.ah_attr.port_num = ib_port;
  rtr_attr.ah_attr.is_global = 1;
  rtr_attr.ah_attr.grh.dgid=remoteGid;
  rtr_attr.ah_attr.grh.flow_label=0;
  rtr_attr.ah_attr.grh.hop_limit=1;
  rtr_attr.ah_attr.grh.sgid_index=3;
  rtr_attr.ah_attr.grh.traffic_class=0;
  if(ibv_modify_qp(queue_pair, &rtr_attr, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER) == 0){
    return true;
  }
  else{
    printf(" RTR Failed \n");
    return false;
  }
}
bool changeQueuePairStateToRTS(struct ibv_qp* queue_pair) {
  struct ibv_qp_attr rts_attr;
  memset(&rts_attr, 0, sizeof(rts_attr));
  rts_attr.qp_state = ibv_qp_state::IBV_QPS_RTS;
  rts_attr.timeout = 12;
  rts_attr.retry_cnt = 7;
  rts_attr.rnr_retry = 7;

  rts_attr.sq_psn = 1217;
  rts_attr.max_rd_atomic = 16;
  if(ibv_modify_qp(queue_pair, &rts_attr, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC) == 0){
    return true;
  }
  else{
    printf("RTS failed\n");
    return false;
  }
}
bool destroy_Queue(struct ibv_qp* qp,struct ibv_cq* cq){
  ibv_destroy_qp(qp);
  ibv_destroy_cq(cq);
}
uint16_t getLocalId(struct ibv_context* context, int ib_port) {
  struct ibv_port_attr port_attr;
  ibv_query_port(context, ib_port, &port_attr);
  return port_attr.lid;
}

uint32_t getQueuePairNumber(struct ibv_qp* qp) {
  return qp->qp_num;
}
