// lock client interface.

#ifndef lock_client_cache_rsm_h

#define lock_client_cache_rsm_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"

#include "rsm_client.h"

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 3.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};

enum cl_rsm_lock_status{
  NONE,
  FREE,
  ACQUIRED,
  ACQUIRING,
  RELEASING
};

class client_rsm_lock{
 public:
  pthread_cond_t cl_rsm_lock_cv;
  cl_rsm_lock_status cl_rsm_lock_state;
  lock_protocol::xid_t xid;
  client_rsm_lock();
  ~client_rsm_lock();
};

class lock_client_cache_rsm;

// Clients that caches locks.  The server can revoke locks using 
// lock_revoke_server.
class lock_client_cache_rsm : public lock_client {
 private:
  rsm_client *rsmc;
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;
  lock_protocol::xid_t xid;
  
  std::map<lock_protocol::lockid_t, client_rsm_lock *> cl_rsm_lock_map;
  pthread_mutex_t cl_rsm_mutex;
  pthread_cond_t cl_rsm_release_cv;
  fifo<lock_protocol::lockid_t> release_queue;

 public:
  static int last_port;
  lock_client_cache_rsm(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache_rsm() {};
  lock_protocol::status acquire(lock_protocol::lockid_t);
  virtual lock_protocol::status release(lock_protocol::lockid_t);
  void releaser();
  rlock_protocol::status revoke_handler(lock_protocol::lockid_t, 
				        lock_protocol::xid_t, int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t, 
				       lock_protocol::xid_t, int &);
};


#endif
