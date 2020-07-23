// just for testing

#include "gen-cpp/CyhgSvc.h"
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <iostream>
#include <string>
#include <memory>
#include <vector>
#include <chrono>
#include <thread>

using namespace apache::thrift::transport;
using namespace apache::thrift::protocol;
using namespace cyhg;

#define RECORDS 1000000

void gen_random(std::string& s, const int len) {
    static const char alphanum[] =
        "ABCDEFGHIJKLMNOPQRSTU"; // ~10000

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    s[len] = 0;
}

int main (int argc, char** argv) {
    using namespace std::chrono;

    int num_threads = 80;
    std::thread thread_pool[num_threads];


    for (int iters=0; iters < 1000; iters++) {
        auto start = high_resolution_clock::now();
        for (int t=0; t<num_threads; t++) {
            thread_pool[t] = std::thread([num_threads]() {
                std::string ip = "localhost";
                uint16_t port = 5700;
                std::shared_ptr<TTransport> socket(new TSocket(ip, port));
                std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
                std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
                CyhgSvcClient client(protocol);
                std::vector<std::string> keys;
                socket->open();
                for (int i=0; i<RECORDS/num_threads; i++) {
                    Record rec;
                    GetResponse gr;
                    gen_random(rec.key, 3);
                    gen_random(rec.value, 10);
                    if ((rand() % 100) <= 20) {
                        client.put(rec);
                    } else {
                        client.get(gr, rec.key);
                    }
                    keys.push_back(rec.key);
                }
                socket->close();
            });
        }

        for (int t=0; t<num_threads; t++) {
            thread_pool[t].join();
        }

        auto stop = high_resolution_clock::now();


        auto duration = duration_cast<microseconds>(stop - start);
        std::cout << duration.count()/1000 << " ms" << std::endl;
        std::cout << RECORDS/(duration.count()/1000.0) << " records / ms" << std::endl;
    }

    return 0;
}

