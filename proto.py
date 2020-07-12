from random import randrange, choice
import time
import threading

DATA_SIZE = 20
WAIT = 0.05
DEBUG = False

def generate_data(n: int) -> dict:
    path = "pg10.txt"
    f = open(path, 'r')
    if f == None:
        print("prob")

    out = {}
    count = 0
    for line in f:
        #out[hash(line)] = line[:-1]
        out[count] = line[:-1]
        count += 1
        if count == DATA_SIZE:
            return out

##

def route_from_hash(hsh: int, curr_server_count: int) -> int:
    return hsh % curr_server_count

class Request:
    def __init__(self, source: int, key: int, header: str, body: list = []):
        self.source = source
        self.key = key
        self.header = header
        self.body = body

    def __repr__(self):
        return str(self.source) + ":" + str(self.key) + ":" + self.header + ":" + str(self.body)

class ServerState:
    def __init__(self, id_num: int):
        self.id_num = id_num
        self.memory = {}
        self.request_queue = []
        self.request_queue_lock = threading.Lock()
        self.known_servers = []
        self.number_of_servers = -1
        self.running = True
        self.serving_requests = True

    def send_message(self, req: Request) -> None:
        self.request_queue_lock.acquire()
        if DEBUG:
            if req != None:
                print(self.id_num,"acquired - append (", req.source, ")")
            else:
                print(self.id_num, "acquired - append (NONE)")
        self.request_queue.append(req)
        self.request_queue_lock.release()
        if DEBUG:
            if req != None:
                print(self.id_num,"released - append (", req.source, ")")
            else:
                print(self.id_num, "released - append (NONE)")

    def pop_queue(self) -> Request:
        self.request_queue_lock.acquire()
        if DEBUG:
            print(self.id_num, "acquired - pop")
        if len(self.request_queue) == 0:
            self.request_queue_lock.release()
            return None
        req = self.request_queue[0]
        self.request_queue = self.request_queue[1:]
        self.request_queue_lock.release()
        if DEBUG:
            print(self.id_num, "released - pop")
        return req

### API
# req
# put
### internode
# req - req is simply forwarded to correct node, same message
# put - put is simply forwarded to correct node, same message
# join
    # number_of_servers
    # known_servers
    # (req)
    # (put)
# join-update
# -leave
# -leave-update
### DEBUG
# stop
# show
# records

def server(initial: bool, reference_server: ServerState, idnum: int) -> None:
    print("server", idnum, ": starting...")
    # initialization
    state = ServerState(idnum)
    if initial:
        state.memory = generate_data(DATA_SIZE)
        state.number_of_servers = 1
        #global
        servers[state.id_num] = state
    else: #join init
        servers[state.id_num] = state
        servers[reference_server].send_message(Request(state.id_num, 0, "join")) #join-update
        while True:
            time.sleep(WAIT)
            ref_response = state.pop_queue()
            if ref_response != None:
                if ref_response.header == "number_of_servers":
                    break
                if ref_response.header == "stop":
                    print("server", state.id_num, ": received stop (premature)")
                    return # @temp
        state.number_of_servers = ref_response.body[0]
        while True:
            time.sleep(WAIT)
            ref_response = state.pop_queue()
            if ref_response != None:
                if ref_response.header == "known_servers":
                    break
                if ref_response.header == "stop":
                    print("server", state.id_num, ": received stop (premature)")
                    return # @temp
        state.known_servers = ref_response.body

    # normal operation loop

    while state.running:
        time.sleep(WAIT)
        if not state.serving_requests: # for testing
            continue
        state.request_queue_lock.acquire()
        if len(state.request_queue) > 0:
            req = state.request_queue[0]
            state.request_queue = state.request_queue[1:]
            state.request_queue_lock.release()
            if req.header == "stop":
                print("server", state.id_num, ": received stop")
                state.running = False
            elif req.header == "print_records":
                multi_line_print_lock.acquire()
                print("server:", state.id_num, "records:")
                for pair in state.memory:
                    print('{:5d}'.format(pair), "|", route_from_hash(pair, state.number_of_servers), "|", state.memory[pair][:40])
                multi_line_print_lock.release()

            elif req.header == "put":
                state.memory[req.key] = req.body[0]
            elif req.header == "get":
                pass

            elif req.header == "join":
                state.number_of_servers += 1
                servers[req.source].send_message(Request(state.id_num,
                    0, "number_of_servers", [state.number_of_servers]))
                server_list_to_send = state.known_servers.copy()
                server_list_to_send.append(state.id_num)
                servers[req.source].send_message(Request(state.id_num,
                    0, "known_servers", server_list_to_send))
                state.send_message(Request(state.id_num, 0, "join_update", [req.source, state.number_of_servers]))
                for serv in state.known_servers:
                    servers[serv].send_message(Request(state.id_num, 0, "join_update",
                        [req.source, state.number_of_servers]))

            elif req.header == "join_update":
                state.number_of_servers = req.body[1]
                state.known_servers.append(req.body[0])
                new_memory = {}
                for record_key in state.memory:
                    dest = route_from_hash(record_key, state.number_of_servers)
                    if dest == state.id_num:
                        new_memory[record_key] = state.memory[record_key]
                    else:
                        servers[dest].send_message(Request(state.id_num, record_key, "put", [state.memory[record_key]]))
                state.memory = new_memory

            else:
                state.send_message(req)
        else:
            state.request_queue_lock.release()

# startup // testing
id_counter = 0
threads = [threading.Thread(target=server, args=(True,None,id_counter,))]
id_counter += 1
servers = {}
servers_lock = threading.Lock()
multi_line_print_lock = threading.Lock()
for t in threads:
    t.start()
# a = threading.Lock() for reference acquire, release
while True:
    command = input(">>")
    if "stop" == command: #sends
        for s in servers:
            servers[s].send_message(Request(-1, 0, "stop"))
        break
    elif "show" == command:
        for s in servers:
            print(s, "|", servers[s].request_queue)
    elif "records" == command:
        for s in servers:
            servers[s].send_message(Request(-1, 0, "print_records"))
    elif "spawn" == command:
        t = threading.Thread(target=server, args=(False, choice(list(servers)), id_counter,))
        threads.append(t)
        t.start()
        id_counter += 1
    elif "spawn2" == command: #the hope ender
        t = threading.Thread(target=server, args=(False, choice(list(servers)), id_counter,))
        threads.append(t)
        t.start()
        id_counter += 1
        t = threading.Thread(target=server, args=(False, choice(list(servers)), id_counter,))
        threads.append(t)
        t.start()
        id_counter += 1
    else:
        print("Unknown command: " + command)

for t in threads:
    t.join()

print("all threads joined")
print("Done!")
