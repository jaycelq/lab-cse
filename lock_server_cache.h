#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>


#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"

enum server_lock_status{FREE,LOCKED};

class server_lock{
 public:
  server_lock_status sv_lock_state;
  std::string owner;
  std::string next_owner;
  std::list<std::string> waiting_list;
  server_lock();
  ~server_lock();
};


class lock_server_cache {
 private:
  int nacquire;
  std::map<std::string, rpcc *> client_map;
  std::map<lock_protocol::lockid_t, server_lock *> sv_lock_map;
  std::list<lock_protocol::lockid_t> redo_list;
  pthread_cond_t revoke_cv;
  pthread_cond_t retry_cv;
  pthread_mutex_t sv_lock;
  bool revoke_flag;
  lock_protocol::lockid_t revoking_lid;

 public:
  lock_server_cache();
  ~lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
  int revoke(lock_protocol::lockid_t);
  int retry(lock_protocol::lockid_t);
};

#endif
