// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

server_lock::server_lock()
{
  sv_lock_state = FREE;
}

server_lock::~server_lock()
{

}

int lock_server_cache::revoke(lock_protocol::lockid_t lid)
{
  int r;
  lock_protocol::status ret = lock_protocol::OK;

  tprintf("Server_cache::client %s is revoking lock %llu\n", sv_lock_map[lid]->owner.c_str(), lid);
  // unlock the mutex to call revoke in case of deadlock
  pthread_mutex_unlock(&sv_lock);
  ret = client_map[sv_lock_map[lid]->owner]->call(rlock_protocol::revoke, lid, r);
  pthread_mutex_lock(&sv_lock);
  VERIFY(ret == lock_protocol::OK || ret == lock_protocol::RELEASE);

  return ret;  
}

int lock_server_cache::retry(lock_protocol::lockid_t lid)
{
  int r;
  std::string client;
  rlock_protocol::status ret = rlock_protocol::OK;
  
  client = sv_lock_map[lid]->waiting_list.front();
  tprintf("Server_cache::client %s is retry lock %llu\n", client.c_str(), lid);
  // unlock the mutex to call retry in case of deadlock
  pthread_mutex_unlock(&sv_lock);
  ret = client_map[client]->call(rlock_protocol::retry, lid, r);
  pthread_mutex_lock(&sv_lock);
  VERIFY(ret == rlock_protocol::OK);
  
  return ret; 
}

lock_server_cache::lock_server_cache()
{
  revoke_flag = true;
  
  pthread_mutex_init(&sv_lock, NULL);
  pthread_cond_init(&revoke_cv, NULL);
  pthread_cond_init(&retry_cv, NULL);
}

lock_server_cache::~lock_server_cache()
{
  pthread_mutex_destroy(&sv_lock);
  pthread_cond_destroy(&revoke_cv);
  pthread_cond_destroy(&retry_cv);
}

int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  bool client_found = false;
  std::list<std::string>::iterator client;
  std::list<lock_protocol::lockid_t>::iterator it;
  lock_protocol::status ret = lock_protocol::OK;
  lock_protocol::status revoke_ret = lock_protocol::OK;

  while(pthread_mutex_trylock(&sv_lock) == EBUSY) usleep(100);

  // bind the client if the client first comes and record it
  if(client_map.count(id) == 0) {
    handle h(id);
    rpcc *cl = h.safebind();
    if(cl) client_map[id] = cl;
    else tprintf("Server_cache::Bind failer\n");
  }

  if(sv_lock_map.count(lid) == 0) {
    sv_lock_map[lid] = new server_lock();
  }
  
  tprintf("Server_cache::client %s is getting lock %llu lock state %d\n",id.c_str() , lid, sv_lock_map[lid]->sv_lock_state);
  switch(sv_lock_map[lid]->sv_lock_state){
    case FREE:
    	// if the lock is free, give it to the client in the front of the waiting_list 
      if(sv_lock_map[lid]->waiting_list.size() == 0 || sv_lock_map[lid]->waiting_list.front() == id){
        sv_lock_map[lid]->sv_lock_state = LOCKED;
        sv_lock_map[lid]->owner = id;
        
        // pop the front client from the waiting_list
        if(sv_lock_map[lid]->waiting_list.size() != 0) {
          sv_lock_map[lid]->waiting_list.pop_front();
        }
        
        // if someone is still waiting for the lock, the client should release the lock without server revoke
        // a new lock state ONCE is introduced to do this	
        if(sv_lock_map[lid]->waiting_list.size() != 0) ret = lock_protocol::ONCE;
        else ret = lock_protocol::OK;
        	
        break;
      }
      
    case LOCKED:
      // add the client to waiting_list if it doesn't exist in waiting_list
      for(client = sv_lock_map[lid]->waiting_list.begin(); client != sv_lock_map[lid]->waiting_list.end(); client++) {
        if(*client == id) {
          client_found = true;
          break;
        }  
      }
      if(client_found == false) sv_lock_map[lid]->waiting_list.push_back(id);
        
      // retry the first client in the waiting_list
      if(id == sv_lock_map[lid]->waiting_list.front()) {
      	revoke_ret = revoke(lid);
      }
      
      // if revoke return new state RELEASE, just release it and client no need to release it server again
      if(revoke_ret == lock_protocol::RELEASE) {
        sv_lock_map[lid]->waiting_list.pop_front();
        sv_lock_map[lid]->owner = id;
        sv_lock_map[lid]->sv_lock_state = LOCKED;
        if(sv_lock_map[lid]->waiting_list.size() != 0) ret = lock_protocol::ONCE;
        else ret = lock_protocol::OK;
      }
      else ret = lock_protocol::RETRY;
      	
      break;
  }
  tprintf("Server_cache::client %s has finishedd getting lock %llu ret %d\n",id.c_str(), lid ,ret);
  pthread_mutex_unlock(&sv_lock);
  return ret;
}

int
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  std::list<lock_protocol::lockid_t>::iterator it;

  while(pthread_mutex_trylock(&sv_lock) == EBUSY) usleep(100);

  if(client_map.count(id) == 0) {
    tprintf("Server_cache::Unknown client\n");
    return ret;
  }

  if(sv_lock_map.count(lid) == 0) {
    tprintf("Server_cache::Unknown lock\n");
    exit(0);
  }
  tprintf("Server_cache::client %s is releasing lock %llu lock state %d\n",id.c_str() , lid, sv_lock_map[lid]->sv_lock_state);
  switch(sv_lock_map[lid]->sv_lock_state){
    case FREE:
      tprintf("Server_cache::Free free lock\n");
      exit(0);

    case LOCKED:
      sv_lock_map[lid]->sv_lock_state = FREE;
      if(sv_lock_map[lid]->waiting_list.size() != 0) ret = retry(lid);
      break;
  }
  tprintf("Server_cache::client %s has released lock %llu\n",id.c_str() , lid);
  pthread_mutex_unlock(&sv_lock);

  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}
