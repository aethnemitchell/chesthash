#include "gen-cpp/CyhgSvc.h"
#include <vector>
#include <string>

int32_t dest_func(const Key& key, int32_t number_of_servers);
Record make_record(const Key& key, const std::string val);
bool check_empty(const std::vector<std::vector<Record>>& r);
size_t check_size(const std::vector<std::vector<Record>>& r);

