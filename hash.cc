// hash.cc

#include <string>

unsigned int string_hash(const std::string& input_string) { // djb2
	unsigned int hash_acc = 5381;
	int constant;

	for (char ch : input_string) {
		hash_acc = ((hash_acc << 5) + hash_acc) + constant; // hash * 33 + c
	}

	return hash_acc;
}

