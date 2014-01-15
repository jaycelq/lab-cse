#ifndef lock_server_cache_rsm_h
#define lock_server_cache_rsm_h

#include <string>

#include "lock_protocol.h"
#include "rpc.h"
#include "rsm_state_transfer.h"
#include "rsm.h"

enum server_rsm_lock_status{FREE,LOCKED};

class server_rsm_lock{
 public:
  bool isRevoking;
  server_rsm_lock_status sv_rsm_lock_state;
  std::string owner;
  std::list<std::string> waiting_list;
  std::map<std::string, lock_protocol::xid_t> xid_map;
  server_rsm_lock();
  ~server_rsm_lock();
};

class lock_server_cache_rsm : public rsm_state_transfer {
 private:
  int nacquire;
  class rsm *rsm;
  std::map<lock_protocol::lockid_t, server_rsm_lock *> sv_rsm_lock_map;
  std::list<lock_protocol::lockid_t> revoke_list;
  std::list<lock_protocol::lockid_t> retry_list;
  pthread_mutex_t sv_rsm_mutex;
  pthread_cond_t retry_cv;
  pthread_cond_t revoke_cv;
  
 public:
  lock_server_cache_rsm(class rsm *rsm = 0);
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  void revoker();
  void retryer();
  std::string marshal_state();
  void unmarshal_state(std::string state);
  int acquire(lock_protocol::lockid_t, std::string id, 
	      lock_protocol::xid_t, int &);
  int release(lock_protocol::lockid_t, std::string id, lock_protocol::xid_t,
	      int &);
};

#endif
