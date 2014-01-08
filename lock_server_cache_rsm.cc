// the caching lock server implementation

#include "lock_server_cache_rsm.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

server_rsm_lock::server_rsm_lock()
{
	sv_rsm_lock_state = FREE;
	isRevoking = false;
}

server_rsm_lock::~server_rsm_lock()
{

}

static void *
revokethread(void *x)
{
  lock_server_cache_rsm *sc = (lock_server_cache_rsm *) x;
  sc->revoker();
  return 0;
}

static void *
retrythread(void *x)
{
  lock_server_cache_rsm *sc = (lock_server_cache_rsm *) x;
  sc->retryer();
  return 0;
}

lock_server_cache_rsm::lock_server_cache_rsm(class rsm *_rsm) 
  : rsm (_rsm)
{
  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  VERIFY (r == 0);
  r = pthread_create(&th, NULL, &retrythread, (void *) this);
  VERIFY (r == 0);
  
  pthread_mutex_init(&sv_rsm_mutex, NULL);
  pthread_cond_init(&revoke_cv, NULL);
  pthread_cond_init(&retry_cv, NULL);
}

void
lock_server_cache_rsm::revoker()
{

  // This method should be a continuous loop, that sends revoke
  // messages to lock holders whenever another client wants the
  // same lock
  int r, ret;
  std::list<lock_protocol::lockid_t>::iterator it;
  lock_protocol::lockid_t lid;
  std::string owner;
    
  pthread_mutex_lock(&sv_rsm_mutex);
  while(true) {
    while(revoke_list.size() == 0) {
      pthread_cond_wait(&revoke_cv, &sv_rsm_mutex);
    }
    for(it = revoke_list.begin(); it != revoke_list.end(); it++){
      lid = *it;
      if(sv_rsm_lock_map[lid]->isRevoking == false && sv_rsm_lock_map[lid]->sv_rsm_lock_state == LOCKED) {
        do {
          owner = sv_rsm_lock_map[lid]->owner;
          tprintf("Server_cache::owner %s is revoking lock %llu xid %llu lock state %d\n", owner.c_str(), lid, sv_rsm_lock_map[lid]->xid_map[owner], sv_rsm_lock_map[lid]->sv_rsm_lock_state);
          ret = client_rsm_map[owner]->call(rlock_protocol::revoke, lid, sv_rsm_lock_map[lid]->xid_map[owner], r);
        } while(ret != lock_protocol::OK);
        sv_rsm_lock_map[lid]->isRevoking = true;
      }
    }
    pthread_cond_wait(&revoke_cv, &sv_rsm_mutex);
  }
}


void
lock_server_cache_rsm::retryer()
{

  // This method should be a continuous loop, waiting for locks
  // to be released and then sending retry messages to those who
  // are waiting for it.
  int r, ret;
  std::list<lock_protocol::lockid_t>::iterator it;
  lock_protocol::lockid_t lid;
  std::string client;
  pthread_mutex_lock(&sv_rsm_mutex);
  while(true) {
    while(retry_list.size() == 0) {
      pthread_cond_wait(&retry_cv, &sv_rsm_mutex);
    }

    for(it = retry_list.begin(); it != retry_list.end();) {
      lid = *it;
      client = sv_rsm_lock_map[lid]->waiting_list.front();
      do {
        tprintf("Server_cache::client %s is retrying lock %llu xid %llu lock state %d\n",client.c_str(), lid, sv_rsm_lock_map[lid]->xid_map[client], sv_rsm_lock_map[lid]->sv_rsm_lock_state);
        ret = client_rsm_map[client]->call(rlock_protocol::retry, lid, sv_rsm_lock_map[lid]->xid_map[client], r); 
      } while(ret != lock_protocol::OK);
      it = retry_list.erase(it);
    }
    
    
  }
}


int lock_server_cache_rsm::acquire(lock_protocol::lockid_t lid, std::string id, 
             lock_protocol::xid_t xid, int &)
{
  bool client_found = false, lock_found = false;
  lock_protocol::status ret = lock_protocol::OK;
  lock_protocol::xid_t xid_last;
  std::list<std::string>::iterator it;
  std::list<lock_protocol::lockid_t>::iterator lock_it;

	if(rsm->amiprimary() == false) return rsm_client_protocol::NOTPRIMARY;
  
  pthread_mutex_lock(&sv_rsm_mutex);
  if(client_rsm_map.count(id) == 0) {
    handle h(id);
    rpcc *cl = h.safebind();
    if(cl) client_rsm_map[id] = cl;
    else {
      tprintf("Server_cache::Bind failure\n");
      exit(0);
    }
  }
  
  if(sv_rsm_lock_map.count(lid) == 0) {
    sv_rsm_lock_map[lid] = new server_rsm_lock();
  }
  
  if(sv_rsm_lock_map[lid]->xid_map.count(id) != 0) {
    xid_last = sv_rsm_lock_map[lid]->xid_map[id];
    if(xid < xid_last) return ret;
  }
  
  switch(sv_rsm_lock_map[lid]->sv_rsm_lock_state) {
    case FREE:
      if(sv_rsm_lock_map[lid]->waiting_list.size() == 0 || sv_rsm_lock_map[lid]->waiting_list.front() == id){
        sv_rsm_lock_map[lid]->sv_rsm_lock_state = LOCKED;
        sv_rsm_lock_map[lid]->owner = id;
        
        for(it = sv_rsm_lock_map[lid]->waiting_list.begin(); it != sv_rsm_lock_map[lid]->waiting_list.end(); it++) {
          if(*it == id) {
            sv_rsm_lock_map[lid]->waiting_list.erase(it);
            break;
          }
        }
        
        if(sv_rsm_lock_map[lid]->waiting_list.size() == 0) {
          for(lock_it = revoke_list.begin(); lock_it != revoke_list.end(); lock_it++) {
            if(*lock_it == lid) {
              revoke_list.erase(lock_it);
              break;
            }
          }
        }
        
        tprintf("Server_cache::client %s is getting lock %llu xid %llu lock state %d waiting_list size %d revoke_list.size %d\n",id.c_str() , lid, xid, sv_rsm_lock_map[lid]->sv_rsm_lock_state, sv_rsm_lock_map[lid]->waiting_list.size(), revoke_list.size());
        if(revoke_list.size() != 0) pthread_cond_broadcast(&revoke_cv);
        
        break;
      }
    case LOCKED:
      for(it = sv_rsm_lock_map[lid]->waiting_list.begin(); it != sv_rsm_lock_map[lid]->waiting_list.end(); it++) {
        if(*it == id) {
          client_found = true;
          break;
        }
      }
      
      if(client_found == false) sv_rsm_lock_map[lid]->waiting_list.push_back(id);

      for(lock_it = revoke_list.begin(); lock_it != revoke_list.end(); lock_it++) {
        if(*lock_it == lid) {
          lock_found = true;
          break;
        }
      }
      if(lock_found == false) revoke_list.push_back(lid);
      tprintf("Server_cache::client %s is getting lock %llu xid %llu lock state %d waiting_list size %d revoke_list.size %d\n",id.c_str() , lid, xid, sv_rsm_lock_map[lid]->sv_rsm_lock_state, sv_rsm_lock_map[lid]->waiting_list.size(), revoke_list.size());  
      pthread_cond_broadcast(&revoke_cv);
        
      ret = lock_protocol::RETRY;
      
  }
  sv_rsm_lock_map[lid]->xid_map[id] = xid;
  pthread_mutex_unlock(&sv_rsm_mutex);
  return ret;
}

int 
lock_server_cache_rsm::release(lock_protocol::lockid_t lid, std::string id, 
         lock_protocol::xid_t xid, int &r)
{
  bool lock_found = false;
  lock_protocol::xid_t xid_last;
  lock_protocol::status ret = lock_protocol::OK;
  std::list<lock_protocol::lockid_t>::iterator it;

	if(rsm->amiprimary() == false) return rsm_client_protocol::NOTPRIMARY;
  
  pthread_mutex_lock(&sv_rsm_mutex);
  if(client_rsm_map.count(id) == 0) {
    tprintf("Sever_cache::Unknown Client\n");
    return ret;
  }
  
  if(sv_rsm_lock_map.count(lid) == 0) {
    tprintf("Sever_cache::Unknown Lock\n");
    return ret;
  }
  
  xid_last = sv_rsm_lock_map[lid]->xid_map[id];
  if(xid < xid_last) return ret;
    
  switch(sv_rsm_lock_map[lid]->sv_rsm_lock_state){
    case FREE:
      tprintf("Server_cache::Free free lock\n");
      return ret;

    case LOCKED:
      sv_rsm_lock_map[lid]->sv_rsm_lock_state = FREE;

      if(sv_rsm_lock_map[lid]->waiting_list.size() != 0) {
        for(it = retry_list.begin(); it != retry_list.end(); it++){
          if(*it == lid) {
            lock_found = true;
            break;
          }
        } 
        if(lock_found == false) retry_list.push_back(lid);
        pthread_cond_broadcast(&retry_cv);
      }
      
      tprintf("Server_cache::client %s is releasing lock %llu xid %llu lock state %d waiting_list.size %d retry_list.size %d\n",id.c_str() , lid, xid, sv_rsm_lock_map[lid]->sv_rsm_lock_state, sv_rsm_lock_map[lid]->waiting_list.size(), retry_list.size());
      sv_rsm_lock_map[lid]->isRevoking = false;
      break;
  }    
  pthread_mutex_unlock(&sv_rsm_mutex);
  return ret;
}

std::string
lock_server_cache_rsm::marshal_state()
{
  std::ostringstream ost;
  std::string r;
  return r;
}

void
lock_server_cache_rsm::unmarshal_state(std::string state)
{
}

lock_protocol::status
lock_server_cache_rsm::stat(lock_protocol::lockid_t lid, int &r)
{
  printf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

