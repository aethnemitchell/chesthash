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
	2:string ip;
	3:i32 port;
}


// ********************
// ***** Services *****
// ********************

service CyhgSvc { // oneway...
	void ping();
	void stop();

	Record get(1:Key key); // return record
	void put(1:Record record);

	map<i32, ServerAddr> join(1:ServerAddr joining_addr);
	void join_update(1:ServerAddr new_addr, 2:i32 new_id, 3:i32 new_number_of_servers);
	void assign_id(1:i32 id);
	void assign_addr(1:ServerAddr addr);
	void initial();

	list<Key> get_keys(); // mostly for debugging

	string info();
}

