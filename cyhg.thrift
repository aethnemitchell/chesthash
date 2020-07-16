namespace cpp cyhg // chest yarn hash guy

// ************************
// ***** Declarations *****
// ************************

typedef string Key

enum RequestType {
	GET,
	PUT,
	JOIN,
	JOIN_UPDATE,
	PING,
	STOP
	//LEAVE
	//LEAVE_UPDATE
}

struct Record {
	1:Key key;
	2:string data;
}

struct ServerAddr {
	1:string ip;
	2:i32 port;
}

struct JoinStruct {
	1: map<i32, ServerAddr> srvm_out;
	2: list<Record> given_records;
}

// ********************
// ***** Services *****
// ********************

service CyhgSvc { // oneway...
	void ping();
	void stop();

	Record get(1:Key key); // return record
	void put(1:Record record);

//	map<i32, ServerAddr> join(1:ServerAddr joining_addr);
	JoinStruct join(1:ServerAddr joining_addr);
	list<Record> join_update(1:ServerAddr new_addr, 2:i32 new_id, 3:i32 new_number_of_srvs, 4:i32 caller_id);
	void assign_id(1:i32 id);
	void assign_addr(1:ServerAddr addr);
	void initial();

	list<Key> get_keys(); // mostly for debugging

	string info();
}

