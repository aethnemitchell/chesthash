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
	if (argc != 3) {
		std::cout << "Arguments: [ip] [port]" << std::endl;
	}
	int32_t port = std::stoi(argv[2]);
	std::string ip = argv[1];

	std::shared_ptr<TTransport> socket(new TSocket(ip, port));
	std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	CyhgSvcClient client(protocol);

	socket->open();

	/*
	 * test shit
	 */

	std::string s;
	client.info(s); //(msg)
	std::cout << "[client] recieved: " << s << std::endl;

	Record rec;
	rec.key = 0;
	rec.data = "wew lad";

	client.put(rec);

	Record rec_recvd;
	client.get(rec_recvd, 0);
	std::cout << rec_recvd.key << ": " << rec_recvd.data << std::endl;

	socket->close();

	return 0;
}

