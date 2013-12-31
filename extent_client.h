// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "extent_server.h"
#include "lock_client_cache.h"

class ec_cache_file {
 public:
  std::string buffer;
  extent_protocol::attr a;
  bool dirty;
  bool buffer_valid;
  bool attr_valid;
  ec_cache_file() : buffer(""), dirty(false), buffer_valid(false), attr_valid(false) {bzero(&a, sizeof(a));}
};

class extent_client : public lock_release_user {
 private:
  rpcc *cl;
  std::map<extent_protocol::extentid_t, ec_cache_file> ec_cache;
  pthread_mutex_t ec_cache_mutex;
 public:
  extent_client(std::string dst);

  extent_protocol::status create(uint32_t type, extent_protocol::extentid_t &eid);
  extent_protocol::status get(extent_protocol::extentid_t eid, 
			                        std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				                          extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);
  extent_protocol::status flush(extent_protocol::extentid_t eid);
  void dorelease(lock_protocol::lockid_t lid);
};

#endif 

