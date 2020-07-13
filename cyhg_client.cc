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

int main () {
	std::shared_ptr<TTransport> socket(new TSocket("localhost", 9090));
	std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	CyhgSvcClient client(protocol);

	socket->open();
	std::string s;
	client.info(s); //(msg)
	std::cout << "[client] recieved: " << s << std::endl;
	socket->close();

	return 0;
}

