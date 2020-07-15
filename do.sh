thrift --gen cpp cyhg.thrift
g++ -std=c++1z cyhg_client.cc gen-cpp/CyhgSvc.cpp gen-cpp/cyhg_types.cpp -o client -lthrift
g++ -std=c++1z cyhg_server.cc gen-cpp/CyhgSvc.cpp gen-cpp/cyhg_types.cpp hash.cc -o server -lthrift
