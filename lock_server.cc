// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_held::lock_held()
{
  pthread_cond_init(&lock_cv, NULL);
  lock_state = false;
}

lock_held::~lock_held()
{
  pthread_cond_destroy(&lock_cv);
}

lock_server::lock_server():
  nacquire (0)
{
  pthread_mutex_init(&lock, NULL);
}

lock_server::~lock_server()
{
  pthread_mutex_destroy(&lock);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  std::map<lock_protocol::lockid_t, lock_held>::iterator it;
  printf("acquire lock from clt %d and lid %llu\n", clt, lid);
  r = nacquire;
  
  pthread_mutex_lock(&lock);
  it = lock_map.find(lid);
  if(it == lock_map.end()){
    lock_held lock_held_new;
    lock_held_new.lock_state = true;
    lock_map.insert(std::pair<lock_protocol::lockid_t, lock_held>(lid, lock_held_new));
  }
  else{
    while(it->second.lock_state == true){
      pthread_cond_wait(&(it->second.lock_cv), &lock);
    }
    it->second.lock_state = true;   
  }
  pthread_mutex_unlock(&lock);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  std::map<lock_protocol::lockid_t, lock_held>::iterator it;
  printf("release lock from clt %d and lid %llu\n", clt, lid);
  r = nacquire;

  pthread_mutex_lock(&lock);
  it = lock_map.find(lid);
  if(it == lock_map.end()){
    ret = lock_protocol::NOENT;
  }
  else{
    if(it->second.lock_state ==false) ret = lock_protocol::IOERR;
    else{
      it->second.lock_state = false;
      pthread_cond_signal(&(it->second.lock_cv));
    }
  }
  pthread_mutex_unlock(&lock);
  return ret;
}

