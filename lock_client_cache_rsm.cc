// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache_rsm.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

#include "rsm_client.h"

client_rsm_lock::client_rsm_lock()
{
  pthread_cond_init(&cl_rsm_lock_cv, NULL);
  cl_rsm_lock_state = NONE;
  xid = 0;
}

client_rsm_lock::~client_rsm_lock()
{
  pthread_cond_destroy(&cl_rsm_lock_cv);
}

static void *
releasethread(void *x)
{
  lock_client_cache_rsm *cc = (lock_client_cache_rsm *) x;
  cc->releaser();
  return 0;
}

int lock_client_cache_rsm::last_port = 0;

lock_client_cache_rsm::lock_client_cache_rsm(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache_rsm::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache_rsm::retry_handler);
  xid = 0;
  // You fill this in Step Two, the part of RSM
  // - Create rsmc, and use the object to do RPC 
  //   calls instead of the rpcc object of lock_client
  rsmc = new rsm_client(xdst);
  pthread_t th;
  int r = pthread_create(&th, NULL, &releasethread, (void *) this);
  VERIFY (r == 0);
  
  pthread_mutex_init(&cl_rsm_mutex, NULL);
  pthread_cond_init(&cl_rsm_release_cv, NULL);
}


void
lock_client_cache_rsm::releaser()
{

  // This method should be a continuous loop, waiting to be notified of
  // freed locks that have been revoked by the server, so that it can
  // send a release RPC.
  int r, ret;
  lock_protocol::lockid_t lid;
  pthread_mutex_lock(&cl_rsm_mutex);
  while(true) {
    while(release_queue.size() == 0) pthread_cond_wait(&cl_rsm_release_cv, &cl_rsm_mutex);
    release_queue.deq(&lid);
    do {
      ret = rsmc->call(lock_protocol::release, lid, id, cl_rsm_lock_map[lid]->xid, r);
    } while(ret != lock_protocol::OK);
    cl_rsm_lock_map[lid]->cl_rsm_lock_state = NONE;
    pthread_cond_broadcast(&(cl_rsm_lock_map[lid]->cl_rsm_lock_cv));
  }
  pthread_mutex_unlock(&cl_rsm_mutex);
}


lock_protocol::status
lock_client_cache_rsm::acquire(lock_protocol::lockid_t lid)
{
  bool isAcquiring = true;
  int ret = lock_protocol::OK, r;
  
  // Use a global mutex to guarantee atomic operation
  pthread_mutex_lock(&cl_rsm_mutex);
  
  // if client never know lock lid, new the client_lock and put it in the cl_lock_map
  if(cl_rsm_lock_map.count(lid) == 0) {
    cl_rsm_lock_map[lid] = new client_rsm_lock();
  }
  
  tprintf("Client_cache::client %s pthread %lu acquire lock %llu state %d\n", id.c_str(), (unsigned long) pthread_self(), lid, cl_rsm_lock_map[lid]->cl_rsm_lock_state);
  
  while(isAcquiring) {
    switch(cl_rsm_lock_map[lid]->cl_rsm_lock_state) {
      // if lock state is NONE, ask server for the lock
      case NONE:
        cl_rsm_lock_map[lid]->cl_rsm_lock_state = ACQUIRING;
        cl_rsm_lock_map[lid]->xid = xid;
        tprintf("Client_cache::client call acquire xid %llu \n", xid);
        ret = rsmc->call(lock_protocol::acquire, lid, id, xid, r);
        // if acquire return RETRY, set lock_state ACQURING and wait until server send retry;
        if(ret == lock_protocol::RETRY) {
          pthread_cond_wait(&(cl_rsm_lock_map[lid]->cl_rsm_lock_cv), &cl_rsm_mutex);
        }
        // else if ret lock_protocol::OK, ++xid and set lock_state FREE
        else if(ret == lock_protocol::OK) {
          xid++;
          cl_rsm_lock_map[lid]->cl_rsm_lock_state = FREE;
        }
        // else the server encounter an error do acquire again
        else {
          cl_rsm_lock_map[lid]->cl_rsm_lock_state = NONE;
        }
        break;
      case FREE:
        cl_rsm_lock_map[lid]->cl_rsm_lock_state = ACQUIRED;
        isAcquiring = false;
        break;
      case ACQUIRING:
      case ACQUIRED:
      case RELEASING:
        pthread_cond_wait(&(cl_rsm_lock_map[lid]->cl_rsm_lock_cv), &cl_rsm_mutex);
        break;
    }
  }
  tprintf("Client_cache::client %s pthread %lu acquire lock %llu state %d\n", id.c_str(), (unsigned long) pthread_self(), lid, cl_rsm_lock_map[lid]->cl_rsm_lock_state);
  pthread_mutex_unlock(&cl_rsm_mutex);

  return ret;
}

lock_protocol::status
lock_client_cache_rsm::release(lock_protocol::lockid_t lid)
{
  int r, ret;
  
  pthread_mutex_lock(&cl_rsm_mutex);
  tprintf("Client_cache::client %s pthread %lu release lock %llu state %d\n", id.c_str(), (unsigned long) pthread_self(), lid, cl_rsm_lock_map[lid]->cl_rsm_lock_state);
  switch(cl_rsm_lock_map[lid]->cl_rsm_lock_state) {
    // should not release a lock that hasn't acquired.
    case NONE:
    case FREE:
    case ACQUIRING:
      tprintf("Client_cache::tring to release unACQUIRED lock\n");
      exit(0);
    // if lock state is ACQUIRED, set it to free and notify that the lock has released in client
    case ACQUIRED:
      cl_rsm_lock_map[lid]->cl_rsm_lock_state = FREE;
      pthread_cond_signal(&(cl_rsm_lock_map[lid]->cl_rsm_lock_cv));
      break;
    // if lock state is RELEASING, release it to server_cache and set the state to NONE 
    // and notify those who are waiting for it
    case RELEASING:
      tprintf("Client_cache::client call release in release xid %llu\n", cl_rsm_lock_map[lid]->xid);
      do{
        ret = rsmc->call(lock_protocol::release, lid, id, cl_rsm_lock_map[lid]->xid, r);
      }
      while(ret != lock_protocol::OK);
      cl_rsm_lock_map[lid]->cl_rsm_lock_state = NONE;
      pthread_cond_broadcast(&(cl_rsm_lock_map[lid]->cl_rsm_lock_cv));
      break;
  }
 
  pthread_mutex_unlock(&cl_rsm_mutex);
  return lock_protocol::OK;

}


rlock_protocol::status
lock_client_cache_rsm::revoke_handler(lock_protocol::lockid_t lid, 
			          lock_protocol::xid_t xid, int &)
{
  int ret = rlock_protocol::OK;
  
  pthread_mutex_lock(&cl_rsm_mutex);
  tprintf("Client_cache::client %s pthread %lu revoke lock %llu xid %llu state %d\n", id.c_str(), (unsigned long) pthread_self(), lid, xid, cl_rsm_lock_map[lid]->cl_rsm_lock_state);
  if(cl_rsm_lock_map.count(lid) == 0) {
    tprintf("Client_cache::trying revoke non-exist lock\n");
    return ret;
  }
  switch(cl_rsm_lock_map[lid]->cl_rsm_lock_state) {
    case NONE:
    case RELEASING:
    case ACQUIRING:
      break;
    case ACQUIRED:
      cl_rsm_lock_map[lid]->cl_rsm_lock_state = RELEASING;
      break;
    case FREE:
      cl_rsm_lock_map[lid]->cl_rsm_lock_state = NONE;
      release_queue.enq(lid);
      pthread_cond_broadcast(&cl_rsm_release_cv);
      break;
  }
  pthread_mutex_unlock(&cl_rsm_mutex);
  
  return ret;
}

rlock_protocol::status
lock_client_cache_rsm::retry_handler(lock_protocol::lockid_t lid, 
			         lock_protocol::xid_t xid, int &)
{
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(&cl_rsm_mutex);
  tprintf("Client_cache::client %s pthread %lu retry lock %llu xid %llu state %d\n", id.c_str(), (unsigned long) pthread_self(), lid, xid, cl_rsm_lock_map[lid]->cl_rsm_lock_state);
  if(cl_rsm_lock_map.count(lid) == 0) {
    tprintf("Client_cache::trying retry non-exist lock\n");
    return ret;
  }   
  switch(cl_rsm_lock_map[lid]->cl_rsm_lock_state) {
    case NONE:
    case ACQUIRED:
    case FREE:
    case RELEASING:
      tprintf("Client_cache::Warning try to retry not ACQUIRING lock\n");
      break;
    case ACQUIRING:
      cl_rsm_lock_map[lid]->cl_rsm_lock_state = NONE;
      pthread_cond_broadcast(&(cl_rsm_lock_map[lid]->cl_rsm_lock_cv));
      break;      
  }
  pthread_mutex_unlock(&cl_rsm_mutex);
  return ret;
}


