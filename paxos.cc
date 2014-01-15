#include "paxos.h"
#include "handle.h"
// #include <signal.h>
#include <stdio.h>
#include "tprintf.h"
#include "lang/verify.h"

// This module implements the proposer and acceptor of the Paxos
// distributed algorithm as described by Lamport's "Paxos Made
// Simple".  To kick off an instance of Paxos, the caller supplies a
// list of nodes, a proposed value, and invokes the proposer.  If the
// majority of the nodes agree on the proposed value after running
// this instance of Paxos, the acceptor invokes the upcall
// paxos_commit to inform higher layers of the agreed value for this
// instance.


bool
operator> (const prop_t &a, const prop_t &b)
{
  return (a.n > b.n || (a.n == b.n && a.m > b.m));
}

bool
operator>= (const prop_t &a, const prop_t &b)
{
  return (a.n > b.n || (a.n == b.n && a.m >= b.m));
}

std::string
print_members(const std::vector<std::string> &nodes)
{
  std::string s;
  s.clear();
  for (unsigned i = 0; i < nodes.size(); i++) {
    s += nodes[i];
    if (i < (nodes.size()-1))
      s += ",";
  }
  return s;
}

bool isamember(std::string m, const std::vector<std::string> &nodes)
{
  for (unsigned i = 0; i < nodes.size(); i++) {
    if (nodes[i] == m) return 1;
  }
  return 0;
}

bool
proposer::isrunning()
{
  bool r;
  ScopedLock ml(&pxs_mutex);
  r = !stable;
  return r;
}

// check if the servers in l2 contains a majority of servers in l1
bool
proposer::majority(const std::vector<std::string> &l1, 
		const std::vector<std::string> &l2)
{
  unsigned n = 0;

  for (unsigned i = 0; i < l1.size(); i++) {
    if (isamember(l1[i], l2))
      n++;
  }
  return n >= (l1.size() >> 1) + 1;
}

proposer::proposer(class paxos_change *_cfg, class acceptor *_acceptor, 
		   std::string _me)
  : cfg(_cfg), acc (_acceptor), me (_me), break1 (false), break2 (false), 
    stable (true)
{
  VERIFY (pthread_mutex_init(&pxs_mutex, NULL) == 0);
  my_n.n = 0;
  my_n.m = me;
}

void
proposer::setn()
{
  my_n.n = acc->get_n_h().n + 1 > my_n.n + 1 ? acc->get_n_h().n + 1 : my_n.n + 1;
}

bool
proposer::run(int instance, std::vector<std::string> cur_nodes, std::string newv)
{
  std::vector<std::string> accepts;
  std::vector<std::string> nodes;
  std::string v;
  bool r = false;

  ScopedLock ml(&pxs_mutex);
  tprintf("start: initiate paxos for %s w. i=%d v=%s stable=%d\n",
	 print_members(cur_nodes).c_str(), instance, newv.c_str(), stable);
  if (!stable) {  // already running proposer?
    tprintf("proposer::run: already running\n");
    return false;
  }
  stable = false;
  setn();
  accepts.clear();
  v.clear();
  if (prepare(instance, accepts, cur_nodes, v)) {

    if (majority(cur_nodes, accepts)) {
      tprintf("paxos::manager: received a majority of prepare responses\n");

      if (v.size() == 0)
	v = newv;

      breakpoint1();

      nodes = accepts;
      accepts.clear();
      accept(instance, accepts, nodes, v);

      if (majority(cur_nodes, accepts)) {
	tprintf("paxos::manager: received a majority of accept responses\n");

	breakpoint2();

	decide(instance, accepts, v);
	r = true;
      } else {
	tprintf("paxos::manager: no majority of accept responses\n");
      }
    } else {
      tprintf("paxos::manager: no majority of prepare responses\n");
    }
  } else {
    tprintf("paxos::manager: prepare is rejected %d\n", stable);
  }
  stable = true;
  return r;
}

// proposer::run() calls prepare to send prepare RPCs to nodes
// and collect responses. if one of those nodes
// replies with an oldinstance, return false.
// otherwise fill in accepts with set of nodes that accepted,
// set v to the v_a with the highest n_a, and return true.
bool
proposer::prepare(unsigned instance, std::vector<std::string> &accepts, 
         std::vector<std::string> nodes,
         std::string &v)
{
  // You fill this in for the part of paxos
  // Note: if got an "oldinstance" reply, commit the instance using
  // acc->commit(...), and return false.
  uint32_t i;
  bool first_loop = true;
  prop_t n_a_max;
  paxos_protocol::status r;

  paxos_protocol::preparearg arg;
  arg.instance = instance;
  arg.n = my_n;
  
  tprintf("paxos::manager: prepare me %s instance %d last proposal instance %d value %s\n", me.c_str(), instance, my_n.n, my_n.m.c_str());
  for(i = 0; i < nodes.size(); i++) {
    tprintf("paxos::manager: node %d %s\n", i, nodes[i].c_str());
    handle h(nodes[i]);
    rpcc* cl = h.safebind();
    if(cl == NULL) continue;
    
    paxos_protocol::prepareres res;
		res.oldinstance = false;
		res.accept = false;
    r = cl->call(paxos_protocol::preparereq, me, arg, res, rpcc::to(1000));
    if(r == paxos_protocol::OK) {
      if(res.oldinstance == true) {
        tprintf("paxos::manager: node %d %s return oldinstance %d %s\n", i, nodes[i].c_str(), res.n_a.n, res.v_a.c_str());
        acc->commit(instance, res.v_a);
        return false;
      }
      else if(res.accept == true) {
        accepts.push_back(nodes[i]);
      }
      if(first_loop || res.n_a > n_a_max) {
          first_loop = false;
          v = res.v_a;
          n_a_max = res.n_a;
      }
    }
  }

  return true;
}

// run() calls this to send out accept RPCs to accepts.
// fill in accepts with list of nodes that accepted.
void
proposer::accept(unsigned instance, std::vector<std::string> &accepts,
        std::vector<std::string> nodes, std::string v)
{
  // You fill this in for the part of paxos
  int r;
  unsigned i;
  bool accept = false;
  paxos_protocol::acceptarg arg;
  arg.instance = instance;
  arg.n = my_n;
  arg.v = v;
  
  tprintf("paxos::manager: accept me %s instance %d value %s\n", me.c_str(), instance, v.c_str());
  for(i = 0; i < nodes.size(); i++){
    tprintf("paxos::manager: node %d %s\n", i, nodes[i].c_str());
    handle h(nodes[i]);
    rpcc* cl = h.safebind();
    if(cl == NULL) continue;
    
    r = cl->call(paxos_protocol::acceptreq, me, arg, accept, rpcc::to(1000));
    if(r == paxos_protocol::OK) {
      if(accept == true) {
        accepts.push_back(nodes[i]);
        accept = false;
      }
    }
  }
}

void
proposer::decide(unsigned instance, std::vector<std::string> accepts, 
	      std::string v)
{
  // You fill this in for the part of paxos
  int ret;
  unsigned i;
  paxos_protocol::decidearg arg;
  arg.instance = instance;
  arg.v = v;
  
  tprintf("paxos::manager: decide me %s instance %d value %s\n", me.c_str(), instance, v.c_str());
  for(i = 0; i < accepts.size(); i++) {
    tprintf("paxos::manager: node %d %s\n", i, accepts[i].c_str());
    handle h(accepts[i]);
    rpcc* cl = h.safebind();
    if(cl == NULL) continue;
    
    cl->call(paxos_protocol::decidereq, me, arg, ret, rpcc::to(1000));
    //VERIFY(r == paxos_protocol::OK);
  }
}

acceptor::acceptor(class paxos_change *_cfg, bool _first, std::string _me, 
	     std::string _value)
  : cfg(_cfg), me (_me), instance_h(0)
{
  VERIFY (pthread_mutex_init(&pxs_mutex, NULL) == 0);

  n_h.n = 0;
  n_h.m = me;
  n_a.n = 0;
  n_a.m = me;
  v_a.clear();

  l = new log (this, me);

  if (instance_h == 0 && _first) {
    values[1] = _value;
    l->loginstance(1, _value);
    instance_h = 1;
  }

  pxs = new rpcs(atoi(_me.c_str()));
  pxs->reg(paxos_protocol::preparereq, this, &acceptor::preparereq);
  pxs->reg(paxos_protocol::acceptreq, this, &acceptor::acceptreq);
  pxs->reg(paxos_protocol::decidereq, this, &acceptor::decidereq);
}

paxos_protocol::status
acceptor::preparereq(std::string src, paxos_protocol::preparearg a,
    paxos_protocol::prepareres &r)
{
  // You fill this in for the part of paxos
  // Remember to initialize *BOTH* r.accept and r.oldinstance appropriately.
  // Remember to *log* the proposal if the proposal is accepted.
  r.accept = false;
  r.oldinstance = false;
  
  tprintf("preparereq: a.n.n %d a.m %s n_h.n %d n_h.m %s %d \n", a.n.n, a.n.m.c_str(), n_h.n, n_h.m.c_str(), a.n >= n_h);
  if(a.instance <= instance_h) {
    r.oldinstance = true;
    r.n_a = n_a;
    r.v_a = values[a.instance];
  }
  else if(a.n >= n_h) {
    n_h = a.n;
    r.accept = true;
    r.n_a = n_a;
    r.v_a = v_a;
    l->logprop(n_h);
  }
  else r.accept = false;
  return paxos_protocol::OK;

}

// the src argument is only for debug purpose
paxos_protocol::status
acceptor::acceptreq(std::string src, paxos_protocol::acceptarg a, bool &r)
{
  // You fill this in for the part of paxos
  // Remember to *log* the accept if the proposal is accepted.
  r = false;
  
  if(a.n >= n_h) {
    n_a = a.n;
    v_a = a.v;
    l->logaccept(n_a, v_a);
    r = true;
  }
  
  return paxos_protocol::OK;
}

// the src argument is only for debug purpose
paxos_protocol::status
acceptor::decidereq(std::string src, paxos_protocol::decidearg a, int &r)
{
  ScopedLock ml(&pxs_mutex);
  tprintf("decidereq for accepted instance %d (my instance %d) v=%s\n", 
	 a.instance, instance_h, v_a.c_str());
  if (a.instance == instance_h + 1) {
    VERIFY(v_a == a.v);
    commit_wo(a.instance, v_a);
  } else if (a.instance <= instance_h) {
    // we are ahead ignore.
  } else {
    // we are behind
    VERIFY(0);
  }
  return paxos_protocol::OK;
}

void
acceptor::commit_wo(unsigned instance, std::string value)
{
  //assume pxs_mutex is held
  tprintf("acceptor::commit: instance=%d has v= %s\n", instance, value.c_str());
  if (instance > instance_h) {
    tprintf("commit: highestaccepteinstance = %d\n", instance);
    values[instance] = value;
    l->loginstance(instance, value);
    instance_h = instance;
    n_h.n = 0;
    n_h.m = me;
    n_a.n = 0;
    n_a.m = me;
    v_a.clear();
    if (cfg) {
      pthread_mutex_unlock(&pxs_mutex);
      cfg->paxos_commit(instance, value);
      pthread_mutex_lock(&pxs_mutex);
    }
  }
}

void
acceptor::commit(unsigned instance, std::string value)
{
  ScopedLock ml(&pxs_mutex);
  commit_wo(instance, value);
}

std::string
acceptor::dump()
{
  return l->dump();
}

void
acceptor::restore(std::string s)
{
  l->restore(s);
  l->logread();
}



// For testing purposes

// Call this from your code between phases prepare and accept of proposer
void
proposer::breakpoint1()
{
  if (break1) {
    tprintf("Dying at breakpoint 1!\n");
    exit(1);
  }
}

// Call this from your code between phases accept and decide of proposer
void
proposer::breakpoint2()
{
  if (break2) {
    tprintf("Dying at breakpoint 2!\n");
    exit(1);
  }
}

void
proposer::breakpoint(int b)
{
  if (b == 3) {
    tprintf("Proposer: breakpoint 1\n");
    break1 = true;
  } else if (b == 4) {
    tprintf("Proposer: breakpoint 2\n");
    break2 = true;
  }
}
