#include <iostream>
#include <thrift/transport/TServerSocket.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/server/TSimpleServer.h>
#include "gen-cpp/CyhgSvc.h"
#include <unordered_map>
#include <string>
 
#define INFO_TEXT "chest-yarn-hash-guy version 0.1 jul-13/2020"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace cyhg;


class CyhgSvcHandler : public CyhgSvcIf {
	std::unordered_map<Key, std::string> record_map;
	std::vector<ServerAddr> known_srv_list;
	int32_t known_num_srvs;
public:
	CyhgSvcHandler() = default;

	void ping() override { std::cout << "ping received" << std::endl; }
	void stop() override { std::cout << "stopping" << std::endl; } // @todo

	void get(Record& rec_out, const Key key) override {
		Record rec;
		if (record_map.count(key) == 0) {
			rec.key = -1;
			rec.data = "";
		} else {
			rec.key = key;
			rec.data = record_map[key];
		}
		rec_out = rec;
	}

	void put(const Record& rec) override {
		record_map[rec.key] = rec.data;
	}

	void join(ServerList& srvl_out) override {
		ServerList srvl;
		srvl.assigned_id = ++known_num_srvs; // @todo actually negotiate
		srvl.srv_addr_list = known_srv_list; // @todo add self @todo figure out addressing
		// @todo call all known servers -> join_update
		// @todo add joiner to known servers
		srvl_out = srvl;
	}

	void join_update(const ServerAddr& new_addr, const int32_t new_number_of_srvs) override {
		known_srv_list.push_back(new_addr);
		known_num_srvs = new_number_of_srvs;
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
	TSimpleServer server(
		std::make_shared<CyhgSvcProcessor>(std::make_shared<CyhgSvcHandler>()),
		std::make_shared<TServerSocket>(9090),
		std::make_shared<TBufferedTransportFactory>(),
		std::make_shared<TBinaryProtocolFactory>()
	);

	std::cout << "Starting the server..." << std::endl;

	server.serve();

	return 1;
}

