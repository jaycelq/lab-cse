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
  pthread_mutex_init(&sv_rsm_mutex, NULL);
  pthread_cond_init(&revoke_cv, NULL);
  pthread_cond_init(&retry_cv, NULL);
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  VERIFY (r == 0);
  r = pthread_create(&th, NULL, &retrythread, (void *) this);
  VERIFY (r == 0);
  
  rsm->set_state_transfer(this); 
  
  tprintf("Server_cache::constructor complete!\n");
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
  tprintf("Server_cache::revoke_thread starting\n");  
  pthread_mutex_lock(&sv_rsm_mutex);
  tprintf("Server_cache::revoke_thread started\n");
  while(true) {
    while(revoke_list.size() == 0) {
      tprintf("Server_cache::revoke_thread wait for the revoke_list to be not empty\n");
      pthread_cond_wait(&revoke_cv, &sv_rsm_mutex);
    }
    
    tprintf("Server_cache::revoke_thread revoke_list size %lu\n", revoke_list.size());
    for(it = revoke_list.begin(); it != revoke_list.end(); it++){
      lid = *it;
      tprintf("Server_cache::revoke_thread lid %llu isRevoking %d lock_state %d\n", lid, sv_rsm_lock_map[lid]->isRevoking, sv_rsm_lock_map[lid]->sv_rsm_lock_state);
      if(sv_rsm_lock_map[lid]->isRevoking == false && sv_rsm_lock_map[lid]->sv_rsm_lock_state == LOCKED) {
        if(rsm->amiprimary() == true) {
          do {
            owner = sv_rsm_lock_map[lid]->owner;
            handle h(owner);
            rpcc *cl = h.safebind();
            if(cl == NULL) {
              tprintf("Server_cache::Bind failure\n");
              break;
            }
            tprintf("Server_cache::owner %s is revoking lock %llu xid %llu lock state %d\n", owner.c_str(), lid, sv_rsm_lock_map[lid]->xid_map[owner], sv_rsm_lock_map[lid]->sv_rsm_lock_state);
            ret = cl->call(rlock_protocol::revoke, lid, sv_rsm_lock_map[lid]->xid_map[owner], r);
          } while(ret != lock_protocol::OK);
        }
        sv_rsm_lock_map[lid]->isRevoking = true;
      }
    }
    tprintf("Server_cache::revoke_thread wait\n");
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
  tprintf("Server_cache::retry_thread starting\n");  
  pthread_mutex_lock(&sv_rsm_mutex);
  tprintf("Server_cache::retry_thread started\n");
  while(true) {
    while(retry_list.size() == 0) {
      tprintf("Server_cache::retry_thread wait for the retry_list to be not empty\n");
      pthread_cond_wait(&retry_cv, &sv_rsm_mutex);
    }
    tprintf("Server_cache::retry_thread retry_list size %lu\n", retry_list.size());
    for(it = retry_list.begin(); it != retry_list.end();) {
      lid = *it;
      client = sv_rsm_lock_map[lid]->waiting_list.front();
      if(rsm->amiprimary() == true) {
        do {
          handle h(client);
          rpcc *cl = h.safebind();
          if(cl == NULL) {
            tprintf("Server_cache::Bind failure\n");
            break;
          }
          tprintf("Server_cache::client %s is retrying lock %llu xid %llu lock state %d\n",client.c_str(), lid, sv_rsm_lock_map[lid]->xid_map[client], sv_rsm_lock_map[lid]->sv_rsm_lock_state);
          ret = cl->call(rlock_protocol::retry, lid, sv_rsm_lock_map[lid]->xid_map[client], r); 
        } while(ret != lock_protocol::OK);
      }
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
  
  pthread_mutex_lock(&sv_rsm_mutex);
  /*
  if(client_rsm_map.count(id) == 0) {
    handle h(id);
    rpcc *cl = h.safebind();
    if(cl) client_rsm_map[id] = cl;
    else {
      tprintf("Server_cache::Bind failure\n");
      exit(0);
    }
  }
  */
  if(sv_rsm_lock_map.count(lid) == 0) {
    sv_rsm_lock_map[lid] = new server_rsm_lock();
  }
  
  tprintf("Server_cache::client %s is getting lock %llu xid %llu lock state %d\n",id.c_str() , lid, xid, sv_rsm_lock_map[lid]->sv_rsm_lock_state);
  if(sv_rsm_lock_map[lid]->xid_map.count(id) != 0) {
    xid_last = sv_rsm_lock_map[lid]->xid_map[id];
    if(xid < xid_last) {
      tprintf("xid %llu of this transcation is smaller than xid_last %llu\n", xid, xid_last);
			pthread_mutex_unlock(&sv_rsm_mutex);
      return ret;
    }
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
        
        tprintf("Server_cache::client %s is getting lock %llu xid %llu lock state %d waiting_list size %lu revoke_list.size %lu\n",id.c_str() , lid, xid, sv_rsm_lock_map[lid]->sv_rsm_lock_state, sv_rsm_lock_map[lid]->waiting_list.size(), revoke_list.size());
        
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
        
      tprintf("Server_cache::client %s is getting lock %llu xid %llu lock state %d waiting_list size %lu revoke_list.size %lu\n",id.c_str() , lid, xid, sv_rsm_lock_map[lid]->sv_rsm_lock_state, sv_rsm_lock_map[lid]->waiting_list.size(), revoke_list.size());    
      pthread_cond_broadcast(&revoke_cv);
        
      ret = lock_protocol::RETRY;
      
  }
  sv_rsm_lock_map[lid]->xid_map[id] = xid;
	tprintf("acquire return\n");
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
  
  pthread_mutex_lock(&sv_rsm_mutex);
  /*
  if(client_rsm_map.count(id) == 0) {
    tprintf("Sever_cache::Unknown Client\n");
    return ret;
  }
  */
  
  if(sv_rsm_lock_map.count(lid) == 0) {
    tprintf("Sever_cache::Unknown Lock\n");
		pthread_mutex_unlock(&sv_rsm_mutex);
    return ret;
  }
  
  xid_last = sv_rsm_lock_map[lid]->xid_map[id];
  if(xid < xid_last) {
    tprintf("xid %llu of this transcation is smaller than xid_last %llu\n", xid, xid_last);
		pthread_mutex_unlock(&sv_rsm_mutex);
    return ret;
  }
    
  tprintf("Server_cache::client %s is releasing lock %llu xid %llu lock state %d\n",id.c_str() , lid, xid, sv_rsm_lock_map[lid]->sv_rsm_lock_state);
  switch(sv_rsm_lock_map[lid]->sv_rsm_lock_state){
    case FREE:
			/*
      tprintf("Server_cache::Free free lock\n");
			pthread_mutex_unlock(&sv_rsm_mutex);
      return ret;
*/
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

      sv_rsm_lock_map[lid]->isRevoking = false;
      break;
  }    
  pthread_mutex_unlock(&sv_rsm_mutex);
  tprintf("release return\n");
  return ret;
}

std::string
lock_server_cache_rsm::marshal_state()
{
  marshall rep;
  
  pthread_mutex_lock(&sv_rsm_mutex);
	tprintf("marshal_state\n");
  rep << (unsigned int)sv_rsm_lock_map.size();
  std::map<lock_protocol::lockid_t, server_rsm_lock *>::iterator it;
  
  for(it = sv_rsm_lock_map.begin(); it != sv_rsm_lock_map.end(); it++) {
    lock_protocol::lockid_t lid = it->first;
    server_rsm_lock *sv_rsm_lock = sv_rsm_lock_map[lid];
    
    rep << lid;
    rep << sv_rsm_lock->isRevoking;
    rep << sv_rsm_lock->sv_rsm_lock_state;
    rep << sv_rsm_lock->owner;
    
    rep << (unsigned int) sv_rsm_lock->waiting_list.size();
    std::list<std::string>::iterator wait_list_it;
    for(wait_list_it = sv_rsm_lock->waiting_list.begin(); wait_list_it != sv_rsm_lock->waiting_list.end(); wait_list_it++) {
      rep << *wait_list_it;
    }
    
    rep << (unsigned int) sv_rsm_lock->xid_map.size();
    std::map<std::string, lock_protocol::xid_t>::iterator xid_it;
    for(xid_it = sv_rsm_lock->xid_map.begin(); xid_it != sv_rsm_lock->xid_map.end(); xid_it++) {
      std::string client = xid_it->first;
      lock_protocol::xid_t xid = sv_rsm_lock->xid_map[client];
      rep << client;
      rep << xid;
    }
  }
  
  rep << (unsigned int) revoke_list.size();
  std::list<lock_protocol::lockid_t>::iterator revoke_list_it;
  for(revoke_list_it = revoke_list.begin(); revoke_list_it != revoke_list.end(); revoke_list_it++) {
    rep << *revoke_list_it;
  }
  
  rep << (unsigned int) retry_list.size();
  std::list<lock_protocol::lockid_t>::iterator retry_list_it;
  for(retry_list_it = retry_list.begin(); retry_list_it != retry_list.end(); retry_list_it++) {
    rep << *retry_list_it;
  }
  
  pthread_mutex_unlock(&sv_rsm_mutex);
  return rep.str();
}

void
lock_server_cache_rsm::unmarshal_state(std::string state)
{
   unsigned int i, j;
  unmarshall rep(state);
  unsigned int lock_size, revoke_list_size, retry_list_size;
  
  pthread_mutex_lock(&sv_rsm_mutex);
  tprintf("unmarshal_state\n");
  rep >> lock_size;
  for(i = 0; i < lock_size; i++) {
    lock_protocol::lockid_t lid;
    rep >> lid;
    if(sv_rsm_lock_map[lid] != NULL) delete sv_rsm_lock_map[lid];
    sv_rsm_lock_map[lid] = new server_rsm_lock();
    rep >> sv_rsm_lock_map[lid]->isRevoking;
    int lock_state;
    rep >> lock_state;
    sv_rsm_lock_map[lid]->sv_rsm_lock_state = (server_rsm_lock_status) lock_state;
    rep >> sv_rsm_lock_map[lid]->owner;
    
    unsigned int wait_list_size;
    rep >> wait_list_size;
    sv_rsm_lock_map[lid]->waiting_list.clear();
    for(j = 0; j < wait_list_size; j++) {
      std::string client;
      rep >> client;
      sv_rsm_lock_map[lid]->waiting_list.push_back(client);
    }
    
    unsigned int xid_map_size;
    rep >> xid_map_size;
    sv_rsm_lock_map[lid]->xid_map.clear();
    for(j = 0; j < xid_map_size; j++) {
      std::string client_xid;
      lock_protocol::xid_t xid;
      rep >> client_xid;
      rep >> xid;
      sv_rsm_lock_map[lid]->xid_map[client_xid] = xid;
    }
  }
  
  rep >> revoke_list_size;
  revoke_list.clear();
  for(i = 0; i < revoke_list_size; i++) {
    lock_protocol::lockid_t lid;
    rep >> lid;
    revoke_list.push_back(lid);
  }
  
  rep >> retry_list_size;
  retry_list.clear();
  for(i = 0; i < retry_list_size; i++) {
    lock_protocol::lockid_t lid;
    rep >> lid;
    retry_list.push_back(lid);
  }
  
  pthread_mutex_unlock(&sv_rsm_mutex);
}

lock_protocol::status
lock_server_cache_rsm::stat(lock_protocol::lockid_t lid, int &r)
{
  printf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

