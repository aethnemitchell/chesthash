namespace cpp cyhg // chest yarn hash guy

// ***** Declarations *****

typedef string Key

struct Record {
    1:required Key key;
    2:string value;
}

struct ServerAddr {
    1:required string ip;
    2:required i32 port;
}

struct JoinStruct {
    1:required ServerAddr assigned_next;
    2:required i32 assigned_id;
    3:required i32 informed_num_srvs;
}

enum StatusCode {
    OK, // all good
    NOT_FOUND, // if not found in the place it should be... shouldnt happen :^)
    NOT_READY // if server is still starting up + being populated
}

struct GetResponse {
    1:required StatusCode status;
    2:Record record;
}

// ***** Services *****

service CyhgSvc {
    void ping(1:i32 source);

    GetResponse get(1: Key key);
    void put(1: Record record);

    oneway void join(1: ServerAddr joining_addr);
    oneway void join_response(1: JoinStruct join_struct);
    oneway void join_update(1: i32 new_num_srvs, 2: list<list<Record>> moving_records);
    oneway void change_next(1: ServerAddr assigned_next);
}

