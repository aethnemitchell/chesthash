// @todo const auto& -> auto& const
// @todo do we need mutex for next_socket etc???

#include <iostream>
#include <map>
#include <string>
#include <chrono>
#include <memory>
#include <mutex>
#include <shared_mutex>

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

typedef tbb::concurrent_unordered_map<Key, std::string> tbb_un_map;

class CyhgSvcHandler : public CyhgSvcIf {
    std::unique_ptr<Logger>             logger;
    tbb_un_map                          record_map;

    int32_t                             node_id;
    ServerAddr                          node_addr;
    bool                                node_init;
    bool                                node_last;
    bool                                node_rdy;

    int32_t                             srvs_in_ring;

    ServerAddr                          next_addr;
    std::shared_ptr<TTransport>         next_socket;
    std::unique_ptr<CyhgSvcClient>      next_rpc_client;

    // only the init node stores these
    ServerAddr                          last_addr;
    std::shared_ptr<TTransport>         last_socket;
    std::unique_ptr<CyhgSvcClient>      last_rpc_client;

    // only non-init nodes store these (init node)
    ServerAddr                          init_addr;
    std::shared_ptr<TTransport>         init_socket;
    std::unique_ptr<CyhgSvcClient>      init_rpc_client;

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
        for (std::vector<Record> v : r) {
            if (v.size() > 0) {
                return false;
            }
        }
        return true;
    }

    size_t check_size(const std::vector<std::vector<Record>>& r) {
        size_t count = 0;
        for (auto v : r) {
            count += v.size();
        }
        return count;
    }

    public:
    CyhgSvcHandler()  = default;

    CyhgSvcHandler(ServerAddr& local_addr, Logger::LogLevel logging_level) {
        // initial node constructor
        logger = std::unique_ptr<Logger>(new Logger(logging_level));
        
        node_id = 0;
        logger->name = "Server " + std::to_string(node_id);
        node_addr = local_addr;
        node_init = true;
        node_last = true;
        node_rdy = true; // no lock needed yet

        srvs_in_ring = 1;


        // TESATING @rem
        record_map["wew1"] = "aksfjkasjfkjalksf";
        record_map["wew2"] = "aksfjkasjfkjalksf";
        record_map["wew3"] = "aksfjkasjfkjalksf";
        record_map["wew4"] = "aksfjkasjfkjalksf";
        record_map["wew5"] = "aksfjkasjfkjalksf";
        record_map["wew6"] = "aksfjkasjfkjalksf";
        record_map["wew7"] = "aksfjkasjfkjalksf";
        record_map["wew8"] = "aksfjkasjfkjalksf";

        logger->log(Logger::Info, "Started.");
    }

    CyhgSvcHandler(ServerAddr&  local_addr, ServerAddr& join_node_addr, Logger::LogLevel logging_level) {
        // joiner node constructor
        logger = std::unique_ptr<Logger>(new Logger(logging_level));
        node_addr = local_addr;
        node_init = false;
        node_last = true;
        node_rdy = false; // no lock needed yet

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

    void join(const ServerAddr& joining_addr) override { // bit of a chungus // @conc
        logger->log(Logger::Debug, "join by " + std::to_string(joining_addr.port));
        if (node_id != 0) logger->log(Logger::Warning, "JOIN ON NON-INIT NODE");

        JoinStruct join_struct;
        join_struct.assigned_next = node_addr;
        join_struct.assigned_id = srvs_in_ring;
        join_struct.informed_num_srvs = ++srvs_in_ring;

        if (srvs_in_ring > 2) {
            last_socket->open();
            last_rpc_client->change_next(joining_addr);
            last_socket->close();
        }

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
        tbb_un_map new_record_map;

        for (const auto& [k, v] : record_map) {
            auto dest = dest_func(k, srvs_in_ring);
            if (dest == node_id) { // aka dest == 0
                new_record_map[k] = v;
            } else {
                moving_records[dest].push_back(make_record(k, v));
                logger->log(Logger::DebugPlus, "join->pack key: " + k + " dest: " + std::to_string(dest));
            }
        }

        record_map = new_record_map;

        next_socket->open();
        next_rpc_client->join_update(srvs_in_ring, moving_records);
        next_socket->close();
    }

    void join_response(const JoinStruct& join_struct) {
        JoinStruct join_struct_recvd = join_struct; // @todo is there a way to not copy? COPY
        node_id = join_struct_recvd.assigned_id;
        logger->name = "Server " + std::to_string(node_id);
        logger->log(Logger::Debug, "join-response, next is now " + std::to_string(join_struct.assigned_next.port));

        srvs_in_ring = join_struct_recvd.informed_num_srvs;
        next_addr = join_struct_recvd.assigned_next;

        next_socket = std::shared_ptr<TTransport>(new TSocket(next_addr.ip, next_addr.port));
        std::shared_ptr<TTransport> next_transport(new TBufferedTransport(next_socket));
        std::shared_ptr<TProtocol> next_protocol(new TBinaryProtocol(next_transport));
        next_rpc_client = std::unique_ptr<CyhgSvcClient>(new CyhgSvcClient(next_protocol));

        logger->log(Logger::Info, "Started.");
    }

    void join_update(int32_t new_num_srvs, const std::vector<std::vector<Record>>& moving_records) override { // @conc
        logger->log(Logger::Debug, "join-update, new num_srvs = " + std::to_string(new_num_srvs));

        // if the list is empty we must be done updating everyone. simply return, dont forward
        if (new_num_srvs == srvs_in_ring && check_empty(moving_records)) {
            node_rdy = true;
            return;
        } 

        node_rdy = false;

        // @todo there must be a better way COPY
        std::vector<std::vector<Record>> new_moving_records = moving_records;

        // we do not need to go through our list and pack new_moving_records further if we
        // are not getting a new new_num_srvs
        // simply take and forward
        // if it is the first time we are seeing this structure, then we must populate it as well, and forward it
        if (new_num_srvs != srvs_in_ring) { // if it IS first time
            tbb_un_map new_record_map;
            for (const auto& [k, v] : record_map) {
                int32_t dest = dest_func(k, new_num_srvs);
                logger->log(Logger::DebugPlus, "join-update->pack key: " + k + " dest: " + std::to_string(dest));
                if (dest == node_id) {
                    new_record_map[k] = v;
                } else {
                    new_moving_records[dest].push_back(make_record(k, v));
                }
            }
            record_map = new_record_map;
        }

        // take out ours either way
        for (const Record& rec : new_moving_records[node_id]) {
            record_map[rec.key] = rec.value;
        }
        new_moving_records[node_id].clear();

        // forward new_moving_records
        next_socket->open();
        next_rpc_client->join_update(new_num_srvs, new_moving_records);
        next_socket->close();

        // if this was our second time seeing this new_moving_records then we can set node_rdy back to true
        if (new_num_srvs == srvs_in_ring) {
            node_rdy = true;
        }

        srvs_in_ring = new_num_srvs;
    }

    void change_next(const ServerAddr& assigned_next) override { // this needs to be somehow synchronized with join_update
        logger->log(Logger::Debug, "change-next to " + std::to_string(assigned_next.port));
        node_rdy = false;
        next_addr = assigned_next;
        next_socket = std::shared_ptr<TTransport>(new TSocket(next_addr.ip, next_addr.port));
        std::shared_ptr<TTransport> next_transport(new TBufferedTransport(next_socket));
        std::shared_ptr<TProtocol> next_protocol(new TBinaryProtocol(next_transport));
        next_rpc_client = std::unique_ptr<CyhgSvcClient>(new CyhgSvcClient(next_protocol));
    }

    void get(GetResponse& response, const Key& key) override {
        //logger->log(Logger::DebugPlus, "get: " + key); // @rem

        response.record.key = "";
        response.record.value = "";

        if (!node_rdy) {
            response.status = StatusCode::NOT_READY;
            return;
        }
        
        if (record_map.count(key) != 0) {
            response.record.key = key;
            response.record.value = record_map[key];
            response.status = StatusCode::OK;
        } else {
            response.status = StatusCode::NOT_FOUND;
        }
    }

    void put(const Record& record) { // @todo node_rdy and response
        //logger->log(Logger::DebugPlus, "put: " + record.key + " - " + record.value); // @rem
        record_map[record.key] = record.value;
    }
};

int main(int argc, char** argv) { // @todo
    bool init_server = argc == 3;

    if (!(argc == 3 || argc == 5)) {
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
                std::make_shared<CyhgSvcProcessor>(std::make_shared<CyhgSvcHandler>(thisAddr, Logger::DebugPlus)),
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
                std::make_shared<CyhgSvcProcessor>(std::make_shared<CyhgSvcHandler>(thisAddr, refAddr, Logger::DebugPlus)),
                std::make_shared<TServerSocket>(this_port),
                std::make_shared<TBufferedTransportFactory>(),
                std::make_shared<TBinaryProtocolFactory>()
                );
        server.serve();
    }

    return 1;
}

