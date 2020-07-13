namespace cpp cyhg // chest yarn hash guyh

// ************************
// ***** Declarations *****
// ************************

const string INFO_TEXT = "chest-yarn-hash-guy-version-0.1"

typedef i32 Key

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
	1:string id;
	2:string ip;
	3:i32 port;
}

struct ServerList { // remains to be seen
	1:i32 assigned_id;
	2:list<ServerAddr> server_addr_list;
}

// ********************
// ***** Services *****
// ********************

service CyhgSvc { // oneway...
	void ping();
	void stop();

	Record get(1:Key key); // return record
	void put(1:Record record);

	ServerList join(); // id should be decided by network at this time, new id = num of servers-1
	void join_update(1:ServerAddr new_addr, 2:i32 new_number_of_servers);

	list<Key> get_keys(); // mostly for debugging

	string info();
}

