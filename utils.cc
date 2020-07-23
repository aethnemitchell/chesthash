#include "utils.h"

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
