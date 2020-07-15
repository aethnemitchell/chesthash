// cyhg_server.cc
//
// 1: itll pay off 
// a: well see
// 1: you dont think it will?
// a: i dont know, anything to keep me out of web dev
// 1: that is really, really, really correct

#include <iostream>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include "gen-cpp/CyhgSvc.h"
#include <map>
#include <string>
#include <chrono>
#include "hash.h"
 
#define INFO_TEXT "chest-yarn-hash-guy version 0.1 jul-15/2020"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace cyhg;

class CyhgSvcHandler : public CyhgSvcIf {
	int32_t id; // set by rpc:assign_id
	ServerAddr own_addr; // set by rpc:assign_addr
	std::map<Key, std::string> record_map; // intialized empty
	int32_t known_num_srvs = 1; // set by rpc:initial
	std::map<int32_t, ServerAddr> known_srv_map;
public:
	CyhgSvcHandler() = default;
	CyhgSvcHandler(ServerAddr& given_addr) {
		own_addr = given_addr;
	}
	CyhgSvcHandler(ServerAddr& given_addr, ServerAddr contact_addr) {
		own_addr = given_addr;
		
		// call join on remote entry server
		std::shared_ptr<TTransport> socket(new TSocket(contact_addr.ip, contact_addr.port));
		std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
		std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
		CyhgSvcClient remote(protocol);
		socket->open();
		remote.join(known_srv_map, own_addr);
		id = known_srv_map.size(); // @todo
		known_num_srvs = id + 1;
		socket->close();
	}

	int32_t dest_func(const Key& key, int32_t number_of_servers) {
		return (string_hash(key) % number_of_servers); // see if i care dude
	}

	void ping() override { std::cout << "ping received" << std::endl; }
	void stop() override { std::cout << "stopping" << std::endl; } // @todo

	// todo get/put multi record
	void get(Record& rec_out, const Key& key) override { // @test
		Record rec;
		int32_t dest = dest_func(key, known_num_srvs);
		std::cout << id << " get: " << key << " dest " << dest << std::endl;
		if (dest != id) {
			ServerAddr target_addr = known_srv_map[dest];
			std::cout << "----redirecting to " << target_addr.ip << std::endl;
			std::shared_ptr<TTransport> socket(new TSocket(
						target_addr.ip,
						target_addr.port));
			std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
			std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
			CyhgSvcClient remote(protocol);
			socket->open();
			Record rec_recvd;
			remote.get(rec_recvd, key);
			socket->close();
			rec = rec_recvd;
		} else {
			rec.key = key;
			rec.data = record_map[key];
		}
		rec_out = rec;
	}

	void put(const Record& rec) override { // @todo redirect
		int32_t dest = dest_func(rec.key, known_num_srvs);
		std::cout << known_num_srvs << " hmm.." << std::endl;
		std::cout << id << " put: " << rec.key << " - " << dest << std::endl;
		if (dest == id) { // put in map
			record_map[rec.key] = rec.data;
		} else { // send to correct peer
			std::cout << "w" << known_srv_map[dest].port << std::endl;
			std::shared_ptr<TTransport> socket(new TSocket(
						known_srv_map[dest].ip,
					 	known_srv_map[dest].port));
			std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
			std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
			CyhgSvcClient remote(protocol);
			socket->open();
			remote.put(rec);
			socket->close();
		}
	}

	void join(std::map<int32_t, ServerAddr>& srvm_out, const ServerAddr& joining_addr) override {
		std::cout << id << " join from " << joining_addr.port << std::endl;
		srvm_out = known_srv_map;
		srvm_out[id] = own_addr;
		int32_t new_id = known_num_srvs;
		known_num_srvs++;

		// notify all other servers
		for (auto [_,sa] : known_srv_map) {
			std::cout << id << " telling " << sa.port << " to join too" << std::endl;
			std::shared_ptr<TTransport> socket(new TSocket(sa.ip, sa.port));
			std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
			std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
			CyhgSvcClient remote(protocol);
			socket->open();
			remote.join_update(joining_addr, new_id, known_num_srvs);
			socket->close();
		}

		known_srv_map[new_id] = joining_addr;

		// distribute files, basically join update but its easier to just duplicate the code
		// more efficient to go 1 peer at a time sorting files by peer dest
		// @todo construct CyhgSvcClient objects/socket objects at join time and store
		for (auto [k,_] : record_map) {
			int32_t dest = dest_func(k, known_num_srvs);
			std::cout << id << " distributing " << k << " to " << dest << std::endl;
			
			ServerAddr sa = known_srv_map[dest];

			std::shared_ptr<TTransport> socket(new TSocket(sa.ip, sa.port));
			std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
			std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
			CyhgSvcClient remote(protocol);
			socket->open();
			Record rec_to_send;
			rec_to_send.key = k;
			rec_to_send.data = record_map[k];
			remote.put(rec_to_send);
			socket->close();
		}
	}

	void join_update(const ServerAddr& new_addr, int32_t new_id, const int32_t new_number_of_srvs) override { // do
		std::cout << id << " join update recvd" << std::endl;

		known_srv_map[new_id] = new_addr;
		known_num_srvs = new_number_of_srvs;
		/*
		for (auto [k,_] : record_map) {
			int32_t dest = dest_func(k, known_num_srvs);
			std::cout << id << " distributing " << k << " to " << dest << std::endl;
			
			sa = known_srv_map[dest];

			std::shared_ptr<TTransport> socket(new TSocket(sa.ip, sa.port));
			std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
			std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
			CyhgSvcClient remote(protocol);
			socket->open();
			remote.put(record_map[k])
			socket->close();
		}
		*/
	}

	void assign_id(const int32_t assigned_id) {
		id = assigned_id;
	}

	void assign_addr(const ServerAddr& assigned_addr) {
		own_addr = assigned_addr;
	}

	void initial() {
		std::cout << "initialized" << std::endl;
		known_num_srvs = 1;
		id = 0;
	}

	void get_keys(std::vector<Key>& keyl_out) override {
		std::vector<Key> keyl;
		for (auto [k,_] : record_map) {
			keyl.push_back(k);
		}
		keyl_out = keyl;
	}

	void info(std::string& s_out) override {
		std::string s = INFO_TEXT;
		s_out = s;
	}
};

int main(int argc, char** argv) {
	
	bool init_server = argc == 3; // @todo

	if (!(argc == 3 || argc == 5)){
		std::cout << "Arguments: [self.ip] [self.port]" << std::endl;
		std::cout << "Arguments: [self.ip] [self.port] [reach.ip] [reach.port]" << std::endl;
		return 0;
	}
	std::string this_ip = argv[1];
	int32_t this_port = std::stoi(argv[2]);

	ServerAddr thisAddr;
	thisAddr.ip = this_ip;
	thisAddr.port = this_port;

	if (init_server) {
		TSimpleServer server(
			std::make_shared<CyhgSvcProcessor>(std::make_shared<CyhgSvcHandler>(thisAddr)),
			std::make_shared<TServerSocket>(this_port),
			std::make_shared<TBufferedTransportFactory>(),
			std::make_shared<TBinaryProtocolFactory>()
		);
		std::cout << "Starting the server on " << this_ip <<  ":" << this_port << "..." << std::endl;

		server.serve();
	} else {
		ServerAddr refAddr;
		refAddr.ip = argv[3];
		refAddr.port = std::stoi(argv[4]);

		TSimpleServer server(
			std::make_shared<CyhgSvcProcessor>(std::make_shared<CyhgSvcHandler>(thisAddr, refAddr)),
			std::make_shared<TServerSocket>(this_port),
			std::make_shared<TBufferedTransportFactory>(),
			std::make_shared<TBinaryProtocolFactory>()
		);
		std::cout << "Starting the server on " << this_ip <<  ":" << this_port << "..." << std::endl;

		server.serve();
	}


	return 1;
}
int32_t JumpConsistentHash(uint64_t key, int32_t num_buckets) {
int64_t b = ­1, j = 0;
while (j < num_buckets) {
b = j;
key = key * 2862933555777941757ULL + 1;
j = (b + 1) * (double(1LL << 31) / double((key >> 33) + 1));
}
return b;
}
