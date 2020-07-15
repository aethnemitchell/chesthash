// hash.cc

#include <string>

int32_t string_hash(const std::string& input_string) { // djb2
	int32_t hash_acc = 0;

	for (char ch : input_string) {
		hash_acc += ch;
	}

	return hash_acc;
}
