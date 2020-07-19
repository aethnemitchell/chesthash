#include <iostream>
#include <map>
#include <string>
#include <chrono>
#include <memory>

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>

#include "tbb/concurrent_unordered_map.h"

#include "gen-cpp/CyhgSvc.h"
#include "hash.h"
#include "log.h"
 
#define INFO_TEXT "Hash-Guy version 0.3 jul-17/2020"
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace cyhg;

typedef tbb::concurrent_unordered_map<std::string, std::string> tbb_un_map;

class CyhgSvcHandler : public CyhgSvcIf {
	std::unique_ptr<Logger> 			logger;
	tbb_un_map							record_map;

	int32_t 							node_id;
	ServerAddr 							node_addr;
	bool								node_init;
	bool								node_last;
	bool								node_rdy;

	int32_t								srvs_in_ring;

	ServerAddr							next_addr;
	std::shared_ptr<TTransport> 		next_socket;
	std::unique_ptr<CyhgSvcClient>		next_rpc_client;

	// only the init node stores these
	ServerAddr							last_addr;
	std::shared_ptr<TTransport> 		last_socket;
	std::unique_ptr<CyhgSvcClient>		last_rpc_client;

	// only non-init nodes store these (init node)
	ServerAddr							init_addr;
	std::shared_ptr<TTransport> 		init_socket;
	std::unique_ptr<CyhgSvcClient>		init_rpc_client;

	int32_t dest_func(const Key& key, int32_t number_of_servers) {
		return (string_hash(key) % number_of_servers);
	}

	Record make_record(const Key& key, const std::string val) {
		Record rec;
		rec.key = key;
		rec.value = val;
		return rec;
	}

	bool check_empty(const std::vector<std::vector<Record>>& r) {
		for (auto v : r) {
			if (v.size() > 0) {
				return false;
			}
		}
		return true;
	}

public:
	CyhgSvcHandler()  = default;

	CyhgSvcHandler(ServerAddr& local_addr, Logger::LogLevel logging_level) {
	// initial node constructor
		node_id = 0;
		logger->name = "Server " + std::to_string(node_id);
		logger->log_level = logging_level;
		node_addr = local_addr;
		node_init = true;
		node_last = true;
		node_rdy = true;

		srvs_in_ring = 1;

		logger->log(Logger::Info, "Started.");
	}

	CyhgSvcHandler(ServerAddr&  local_addr, ServerAddr& join_node_addr, Logger::LogLevel logging_level) {
	// joiner node constructor
		node_addr = local_addr;
		node_init = false;
		node_last = true;
		node_rdy = false;
		logger->log_level = logging_level;

		// contact init node for network data, populate fields
		init_addr = join_node_addr;
		init_socket = std::shared_ptr<TTransport>(new TSocket(init_addr.ip, init_addr.port));
		std::shared_ptr<TTransport> init_transport(new TBufferedTransport(init_socket));
		std::shared_ptr<TProtocol> init_protocol(new TBinaryProtocol(init_transport));
		init_rpc_client = std::unique_ptr<CyhgSvcClient>(new CyhgSvcClient(init_protocol));

		init_socket->open();
		init_rpc_client->join(node_addr);
		init_socket->close();
	}

	void ping(int32_t source) override {
		logger->log(Logger::Info, "pinged by " + std::to_string(source));
	}

	void join(const ServerAddr& joining_addr) override { // bit of a chungus
		logger->log(Logger::Debug, "join");
		if (node_id != 0) logger->log(Logger::Warning, "JOIN ON NON-INIT NODE");

		JoinStruct join_struct;
		join_struct.assigned_next = node_addr;
		join_struct.assigned_id = srvs_in_ring;
		join_struct.informed_num_srvs = ++srvs_in_ring;

		last_addr = joining_addr;
		last_socket = std::shared_ptr<TTransport>(new TSocket(last_addr.ip, last_addr.port));
		std::shared_ptr<TTransport> last_transport(new TBufferedTransport(last_socket));
		std::shared_ptr<TProtocol> last_protocol(new TBinaryProtocol(last_transport));
		last_rpc_client = std::unique_ptr<CyhgSvcClient>(new CyhgSvcClient(last_protocol));

		last_socket->open();
		last_rpc_client->join_response(join_struct);
		last_socket->close();
		
		// new server has been intialized.
		// repopulate and inform

		if (srvs_in_ring == 2) {
			next_addr = joining_addr;
			next_socket = std::shared_ptr<TTransport>(new TSocket(next_addr.ip, next_addr.port));
			std::shared_ptr<TTransport> next_transport(new TBufferedTransport(next_socket));
			std::shared_ptr<TProtocol> next_protocol(new TBinaryProtocol(next_transport));
			next_rpc_client = std::unique_ptr<CyhgSvcClient>(new CyhgSvcClient(next_protocol));
		}
		
		std::vector<std::vector<Record>> moving_records(srvs_in_ring);
		std::map<Key, std::string> new_record_map;

		for (const auto& [k, v] : record_map) {
			auto dest = dest_func(k, srvs_in_ring);
			if (dest == node_id) { // aka dest == 0
				new_record_map[k] = v;
			} else {
				moving_records[dest].push_back(make_record(k, v));
			}
		}

		next_socket->open();
		next_rpc_client->join_update(srvs_in_ring, moving_records);
		next_socket->close();
	}

	void join_response(const JoinStruct& join_struct) {
		JoinStruct join_struct_recvd = join_struct; // @todo lazy
		node_id = join_struct_recvd.assigned_id;
		logger->name = "Server " + std::to_string(node_id);
		logger->log(Logger::Debug, "join-response");

		srvs_in_ring = join_struct_recvd.informed_num_srvs;
		next_addr = join_struct_recvd.assigned_next;

		next_socket = std::shared_ptr<TTransport>(new TSocket(next_addr.ip, next_addr.port));
		std::shared_ptr<TTransport> next_transport(new TBufferedTransport(next_socket));
		std::shared_ptr<TProtocol> next_protocol(new TBinaryProtocol(next_transport));
		next_rpc_client = std::unique_ptr<CyhgSvcClient>(new CyhgSvcClient(next_protocol));

		logger->log(Logger::Info, "Started.");
	}

	void join_update(int32_t new_num_srvs, const std::vector<std::vector<Record>>& moving_records) override {
		logger->log(Logger::Debug, "join-update");

		if (check_empty(moving_records)) {
			return;
		} 

		// we do not need to go through our list and pack moving_records further if we
		// are not getting a new new_num_srvs
		// simply take and forward
		
		if (new_num_srvs == srvs_in_ring) {
			for (const Record& rec : moving_records[node_id]) {
				record_map[rec.key] = rec.value;
			}
		}

		// if it is the first time we are seeing this structure, then we must populate it as well
		
		std::map<Key, std::string> new_record_map;
	}

	void change_next(const ServerAddr& assigned_next) override {
		logger->log(Logger::Debug, "change-next");
		next_addr = assigned_next;
		next_socket = std::shared_ptr<TTransport>(new TSocket(next_addr.ip, next_addr.port));
		std::shared_ptr<TTransport> next_transport(new TBufferedTransport(next_socket));
		std::shared_ptr<TProtocol> next_protocol(new TBinaryProtocol(next_transport));
		next_rpc_client = std::unique_ptr<CyhgSvcClient>(new CyhgSvcClient(next_protocol));
		// memory leak? @fix
	}

	void get(GetResponse& response, const Key& key) override { // @todo not found etc
		if (record_map.count(key) != 0) {
			response.record.key = key;
			response.record.value = record_map[key];
			response.status = StatusCode::OK;
		} else {
			response.status = StatusCode::NOT_FOUND;
		}
	}

	void put(const Record& record) {
		record_map[record.key] = record.value;
	}
};

int main(int argc, char** argv) { // @todo
	bool init_server = argc == 3;

	if (!(argc == 3 || argc == 5)){
		std::cout << "Arguments: [self.ip] [self.port]" << std::endl;
		std::cout << "Arguments: [self.ip] [self.port] [init.ip] [init.port]" << std::endl;
		return 0;
	}
	std::string this_ip = argv[1];
	int32_t this_port = std::stoi(argv[2]);

	ServerAddr thisAddr;
	thisAddr.ip = this_ip;
	thisAddr.port = this_port;

	if (init_server) {
		TThreadedServer server(
			std::make_shared<CyhgSvcProcessor>(std::make_shared<CyhgSvcHandler>(thisAddr, Logger::Debug)),
			std::make_shared<TServerSocket>(this_port),
			std::make_shared<TBufferedTransportFactory>(),
			std::make_shared<TBinaryProtocolFactory>()
		);
		server.serve();
	} else {
		ServerAddr refAddr;
		refAddr.ip = argv[3];
		refAddr.port = std::stoi(argv[4]);

		TThreadedServer server(
			std::make_shared<CyhgSvcProcessor>(std::make_shared<CyhgSvcHandler>(thisAddr, refAddr, Logger::Debug)),
			std::make_shared<TServerSocket>(this_port),
			std::make_shared<TBufferedTransportFactory>(),
			std::make_shared<TBinaryProtocolFactory>()
		);
		server.serve();
	}

	return 1;
}
