// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
  pthread_mutex_init(&ec_cache_mutex, NULL);
}

// a demo to show how to use RPC
extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&ec_cache_mutex);
  if(ec_cache.count(eid) == 0 || ec_cache[eid].attr_valid == false) {
    ret = cl->call(extent_protocol::getattr, eid, attr);
    if(ret != extent_protocol::OK) {
      printf("ERROR: getting attr %d\n", ret);
      pthread_mutex_unlock(&ec_cache_mutex);
      return ret;
    }
    else {
      ec_cache[eid].a = attr;
      ec_cache[eid].attr_valid = true;
    }   
  }
  else attr = ec_cache[eid].a;
  
  pthread_mutex_unlock(&ec_cache_mutex);
  return ret;
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab3 code goes here
  pthread_mutex_lock(&ec_cache_mutex);
  ret = cl->call(extent_protocol::create, type, id);
  pthread_mutex_unlock(&ec_cache_mutex);
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  
  pthread_mutex_lock(&ec_cache_mutex);  
  // if extent cache doesn't hit or cache is not valid, ask the content from the extent server
  if(ec_cache.count(eid) == 0 || ec_cache[eid].buffer_valid == false) {
    ret = cl->call(extent_protocol::get, eid, buf);
    if(ret != extent_protocol::OK) {
      printf("ERROR: getting attr %d\n", ret);
      pthread_mutex_unlock(&ec_cache_mutex);
      return ret;
    }
    ec_cache[eid].buffer = buf;
    ec_cache[eid].buffer_valid = true;
    ec_cache[eid].dirty = false;
    // Theoretically, cache should have attr before call get
    if(ec_cache[eid].attr_valid == true) ec_cache[eid].a.atime = time(NULL);
    else printf("extent_client_cache_fault::no attr info while get buffer\n");
  }
  else buf = ec_cache[eid].buffer;
  
  pthread_mutex_unlock(&ec_cache_mutex);  
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
    
  pthread_mutex_lock(&ec_cache_mutex);
  
  ec_cache[eid].a.size = buf.size();
  ec_cache[eid].a.ctime = time(NULL);
  ec_cache[eid].a.mtime = time(NULL);
  ec_cache[eid].buffer = buf;
  ec_cache[eid].dirty = true;
  ec_cache[eid].buffer_valid = true;
  
  pthread_mutex_unlock(&ec_cache_mutex);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab3 code goes here
  int r;
  pthread_mutex_lock(&ec_cache_mutex);
  printf("extent_client_cache_info::remove file %llu is removed from cache\n", eid);
  ret = cl->call(extent_protocol::remove, eid, r);
  ec_cache.erase(eid);
  pthread_mutex_unlock(&ec_cache_mutex);
  return ret;
}

extent_protocol::status
extent_client::flush(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  
  pthread_mutex_lock(&ec_cache_mutex);
  // if the ec_cache.count(eid) == 0, the node has been removed
  if(ec_cache.count(eid) == 0) {
    printf("extent_client_cache_info::flush file %llu has been removed from cache\n", eid);
    pthread_mutex_unlock(&ec_cache_mutex);
    return ret;
  }
  
  if(ec_cache[eid].buffer_valid == true) {
    if(ec_cache[eid].dirty == true) 
      ret = cl->call(extent_protocol::put, eid, ec_cache[eid].buffer, r);
    else printf("extent_client_cache_info::flush file %llu need do nothing\n", eid);
  }
  else {
    printf("extent_client_cache_fault::flush file %llu buffer not valid\n", eid);
  }
  ec_cache.erase(eid);  
  pthread_mutex_unlock(&ec_cache_mutex);  
  
  return ret;
}

void
extent_client::dorelease(lock_protocol::lockid_t lid)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = flush(lid);
  printf("extent_client_cache_info::dorelease file %llu  ret %d\n", lid, ret);
}

