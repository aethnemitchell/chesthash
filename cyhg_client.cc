#include "gen-cpp/CyhgSvc.h"
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <iostream>
#include <string>
#include <memory>
#include <vector>

using namespace apache::thrift::transport;
using namespace apache::thrift::protocol;
using namespace cyhg;

int main (int argc, char** argv) {
	if (argc != 2) {
		std::cout << "Arguments: [ip]" << std::endl;
	}

	std::string ip = argv[1];

	int port;
	int req; // 0 get 1 put
	std::string key;
	std::string data;

	std::cout << "port 01getput" << std::endl;
		
	while (true) {
		std::cin >> port >> req;
		if (req == 0) {
			std::cout << "key: " << std::endl;
			std::cin >> key;
		} else {
			std::cout << "key value: " << std::endl;
			std::cin >> key >> data;
		}

		std::shared_ptr<TTransport> socket(new TSocket(ip, port));
		std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
		std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
		CyhgSvcClient client(protocol);

		socket->open();

		/*
		 * test shit
		 */

		std::string s;

		if (req == 1) {
			Record rec;
			rec.key = key;
			rec.data = data;
			client.put(rec);
		} else if (req == 0) {
			Record rec_recvd;
			client.get(rec_recvd, key);
			std::cout << rec_recvd.key << ": " << rec_recvd.data << " from " << port << std::endl;
		} else if (req == 9) {
			break;
		}

		socket->close();
	}

	return 0;
}

