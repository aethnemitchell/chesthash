// cyhg_server.cc
//
// c: itll pay off 
// a: well see
// c: you dont think it will
// a: i dont know, anything to keep me out of web dev
// c: that is really, really, really correct

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
#include
 
#define INFO_TEXT "chest-yarn-hash-guy version 0.1 jul-13/2020"

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
	std::hash<std::string> string_hash;
public:
	CyhgSvcHandler() = default;

	int32_t dist_func(const Key& key, int32_t number_of_servers) {
		return (string_hash(key) % number_of_servers); // see if i care dude
	}

	void ping() override { std::cout << "ping received" << std::endl; }
	void stop() override { std::cout << "stopping" << std::endl; } // @todo

	void get(Record& rec_out, const Key& key) override { // @test
		Record rec;
		std::cout << known_num_srvs << "?" << std::endl;
		int32_t dest = dist_func(key, known_num_srvs);
		std::cout << dest <<":"<<key << std::endl;
		if (dest != id) {
			ServerAddr target_addr = known_srv_map[dest];
			std::shared_ptr<TTransport> socket(new TSocket(
						target_addr.ip, target_addr.port)
					);
			std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
			std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
			CyhgSvcClient client(protocol);
			socket->open();
			Record rec_recvd;
			client.get(rec_recvd, key);
			socket->close();
			rec = rec_recvd;
		} else {
			rec.key = key;
			rec.data = record_map[key];
		}
		rec_out = rec;
	}

	void put(const Record& rec) override { // @todo redirect
		std::cout << "put: " << rec.key << std::endl;
		record_map[rec.key] = rec.data;
	}

	void join(std::map<int32_t, ServerAddr>& srvm_out, const ServerAddr& self_addr) override { // @todo all of it
		srvm_out = known_srv_map;
		known_srv_map[2] = self_addr;
	}

	void join_update(const ServerAddr& new_addr, int32_t new_id, const int32_t new_number_of_srvs) override {
		known_srv_map[new_id] = new_addr;
		known_num_srvs = new_number_of_srvs;
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

int main(int argc, char** argv) { // main function is gonna initialize server once its running!
	if (!(argc == 3 || argc == 5)){
		std::cout << "Arguments: [self.ip] [self.port]" << std::endl;
		std::cout << "Arguments: [self.ip] [self.port] [reach.ip] [reach.port]" << std::endl;
		return 0;
	}
	// @todo non-intial
	std::string this_ip = argv[1];
	int32_t this_port = std::stoi(argv[2]);

	TSimpleServer server(
		std::make_shared<CyhgSvcProcessor>(std::make_shared<CyhgSvcHandler>()),
		std::make_shared<TServerSocket>(this_port),
		std::make_shared<TBufferedTransportFactory>(),
		std::make_shared<TBinaryProtocolFactory>()
	);

	std::cout << "Starting the server on " << this_ip <<  ":" << this_port << "..." << std::endl;
	
	server.serve();

	std::shared_ptr<TTransport> socket(new TSocket(this_ip, this_port));
	std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	CyhgSvcClient client(protocol);
	socket->open();

	if (argc == 3) {
		// initialize
		ServerAddr thisAddr;
		thisAddr.ip = this_ip;;
		thisAddr.port = this_port;

		// intialize stuff

		client.initial();
		client.assign_addr(thisAddr);
	}

	socket->close();

	return 1;
}

/*
std::shared_ptr<TTransport> socket(new TSocket(remote_ip, remote_port));
std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
CyhgSvcClient client(protocol);
socket->open();
client. ();
socket->close();
*/
