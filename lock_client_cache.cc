// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


int lock_client_cache::last_port = 0;

client_lock::client_lock()
{
  pthread_cond_init(&cl_lock_cv, NULL);
  cl_lock_state = NONE;
}

client_lock::~client_lock()
{
  pthread_cond_destroy(&cl_lock_cv);
}

lock_client_cache::lock_client_cache(std::string xdst, 
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
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
  
  is_acquire = false;
  pthread_mutex_init(&cl_lock, NULL);
  pthread_cond_init(&acquire_cv, NULL);
}

lock_client_cache::~lock_client_cache()
{
  pthread_mutex_destroy(&cl_lock);
  pthread_cond_destroy(&acquire_cv);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int r;
  bool lock_flag = false;
  lock_protocol::status retval;

  pthread_mutex_lock(&cl_lock);
  
  // if someone is acquiring, release the mutex and wait for acquire_cv
  if(is_acquire == true) pthread_cond_wait(&acquire_cv, &cl_lock);
  else is_acquire = true;
  
  // if client never know lock lid, new the client_lock and put it in the cl_lock_map
  if(cl_lock_map.count(lid) == 0) {
    cl_lock_map[lid] = new client_lock();
  }
  
  // acquire finish until client get the lock
  while(lock_flag == false){
  	tprintf("Client_cache::client %s pthread %lu acquire lock %llu state %d\n", id.c_str(), (unsigned long) pthread_self(), lid, cl_lock_map[lid]->cl_lock_state);
    switch(cl_lock_map[lid]->cl_lock_state){
      // if lock state is NONE, ask server for the lock
      case NONE:
        cl_lock_map[lid]->cl_lock_state = ACQUIRING;
        
        pthread_mutex_unlock(&cl_lock);
        retval = cl->call(lock_protocol::acquire, lid, id, r);
        pthread_mutex_lock(&cl_lock);
        VERIFY(retval == lock_protocol::OK || retval == lock_protocol::RETRY || retval == lock_protocol::ONCE);
        
        // lock state can be modified by revoke and retry, but can't be modified by release
        if(cl_lock_map[lid]->cl_lock_state == RELEASING) retval = lock_protocol::ONCE;
        else if(cl_lock_map[lid]->cl_lock_state == NONE) break;
        
        // if retval is RETRY, wait for the cl_lock_cv to acquire from server again or get the lock directly
        if(retval == lock_protocol::OK || retval == lock_protocol::ONCE ) cl_lock_map[lid]->cl_lock_state = FREE;
        else {
          is_acquire = false;
          pthread_cond_broadcast(&acquire_cv);
          pthread_cond_wait(&(cl_lock_map[lid]->cl_lock_cv), &cl_lock);
        }
        break;
        
      // if lock state is FREE, set the lock ACQUIRED 
      case FREE:
        cl_lock_map[lid]->cl_lock_state = ACQUIRED;
        lock_flag = true;
        break;
      
      // if lock state is ACQUIRING, keep what it is and wait for the retry condtion
      case ACQUIRING:
        is_acquire = false;
        pthread_cond_broadcast(&acquire_cv);
        pthread_cond_wait(&(cl_lock_map[lid]->cl_lock_cv), &cl_lock);
        break;
      
      // if lock state is ACQUIRED, keep what it is and wait for client to release it
      case ACQUIRED:
        is_acquire = false;
        pthread_cond_broadcast(&acquire_cv);
        pthread_cond_wait(&(cl_lock_map[lid]->cl_lock_cv), &cl_lock);
        break;

      // if lock state is RELEASING, wait until client_cache release it and acquire it again
      case RELEASING:
        is_acquire = false;
        pthread_cond_broadcast(&acquire_cv);
        pthread_cond_wait(&(cl_lock_map[lid]->cl_lock_cv), &cl_lock);
        break;
    }
  }
  
  if(retval == lock_protocol::ONCE) cl_lock_map[lid]->cl_lock_state = RELEASING;
  tprintf("Client_cache::client %s pthread %lu after acquire lock %llu state %d\n", id.c_str(), (unsigned long) pthread_self(), lid, cl_lock_map[lid]->cl_lock_state);
  
  is_acquire = false;
  pthread_cond_broadcast(&acquire_cv);
  pthread_mutex_unlock(&cl_lock);

  return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int r;
  lock_protocol::status ret;
  
  pthread_mutex_lock(&cl_lock);

  if(cl_lock_map.count(lid) == 0) {
    printf("Client_cache::tring to release non-exist lock\n");
    exit(0);
  }
  tprintf("Client_cache::client %s pthread %lu release lock %llu state %d\n", id.c_str(), (unsigned long) pthread_self(), lid, cl_lock_map[lid]->cl_lock_state);
  switch(cl_lock_map[lid]->cl_lock_state){
    // should not release a lock that hasn't acquired.
    case NONE:
    case FREE:
    case ACQUIRING:
      tprintf("Client_cache::tring to release unACQUIRED lock\n");
      exit(0);
    
    // if lock state is ACQUIRED, set it to free and notify that the lock has released in client
    case ACQUIRED:
      cl_lock_map[lid]->cl_lock_state = FREE;
      pthread_cond_signal(&(cl_lock_map[lid]->cl_lock_cv));
      break;

    // if lock state is RELEASING, release it to server_cache and set the state to NONE 
    // and notify those who are waiting for it
    case RELEASING:
      lu->dorelease(lid);
      pthread_mutex_unlock(&cl_lock);
      ret = cl->call(lock_protocol::release, lid, id, r);
      pthread_mutex_lock(&cl_lock);
      VERIFY(ret == lock_protocol::OK);
      cl_lock_map[lid]->cl_lock_state = NONE;
      pthread_cond_broadcast(&(cl_lock_map[lid]->cl_lock_cv));
      break;
  }
  tprintf("Client_cache::client %s pthread %lu after release lock %llu state %d\n", id.c_str(), (unsigned long) pthread_self(), lid, cl_lock_map[lid]->cl_lock_state);
  pthread_mutex_unlock(&cl_lock);
  
  return lock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int & )
{
  rlock_protocol::status ret = rlock_protocol::OK;

  pthread_mutex_lock(&cl_lock);
  tprintf("Client_cache::client %s revoke lock %llu state %d\n", id.c_str(), lid, cl_lock_map[lid]->cl_lock_state);
  if (cl_lock_map.count(lid) == 0) {
    printf("Client_cache::tring to revoke non-exist lock\n");
    exit(0);
  }

  switch(cl_lock_map[lid]->cl_lock_state){
  	// if lock state is NONE or RELEASING, ignore the revoke request
    case NONE:
    case RELEASING:
      break;
	  
	  // if client_cache acquire the lock, set it to RELEASING, so that 
    case ACQUIRING:
    case ACQUIRED:
      cl_lock_map[lid]->cl_lock_state = RELEASING;
      break;

    // if the the lock is FREE, set it to NONE, and notify server by return lock_protocol::RELEASE
    case FREE:
      lu->dorelease(lid);
      cl_lock_map[lid]->cl_lock_state = NONE;
      pthread_cond_broadcast(&(cl_lock_map[lid]->cl_lock_cv));
	    ret = lock_protocol::RELEASE;
      break;  
  }
  tprintf("Client_cache::client %s is after revoke lock %llu state %d\n", id.c_str(), lid, cl_lock_map[lid]->cl_lock_state);
  pthread_mutex_unlock(&cl_lock);
  
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int ret = rlock_protocol::OK;
  
  pthread_mutex_lock(&cl_lock);
  tprintf("Client_cache::client %s retry lock %llu state %d\n", id.c_str(), lid, cl_lock_map[lid]->cl_lock_state);
  if (cl_lock_map.count(lid) == 0) {
    printf("Client_cache::tring to retry non-exist lock\n");
	  exit(0);
  }
  
  switch(cl_lock_map[lid]->cl_lock_state){
    case NONE:
    case ACQUIRED:
    case FREE:
    case RELEASING:
      tprintf("Client_cache::tring to retry ACQUIRED lock\n");
      break;
	
    case ACQUIRING:
      cl_lock_map[lid]->cl_lock_state = NONE;
      pthread_cond_broadcast(&(cl_lock_map[lid]->cl_lock_cv));
      break;
  }
  tprintf("Client_cache::client %s is after retry lock %llu state %d\n", id.c_str(), lid, cl_lock_map[lid]->cl_lock_state);
  pthread_mutex_unlock(&cl_lock);
  return ret;
}



