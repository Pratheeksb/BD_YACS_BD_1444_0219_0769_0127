"""
Master Node
This program runs on the master.

"""
import json
import random
import time
from threading import Thread
import socket



def sta_val(n):

    sta_val = ["SUCCESS", "PENDING", "FAILED", "ONGOING"]
    return sta_val[n-1]


class Master:
    def __init__(self, config_path, scheduler):
        with open(config_path, 'r') as cfg_file:
            config = json.load(cfg_file)
        self.workers = config['workers']
        for w in self.workers:
            w['slot_free_count'] = w['slots']
        self.scheduler = scheduler
        self.threads = []
        self.connections = []
        self.process_pool = []
        self.request_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.worker_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # new thread for established connections(connections manager)
    def __enter__(self):
        self.re_thread = Thread(target=self.listen_to_request)
        self.re_thread.start()
        self.thread_workers = Thread(target=self.listen_to_worker)
        self.thread_workers.start()
        return self

    # cleaning the resources
    def __exit__(self, exc_type, exc_value, traceback):
        self.request_sock.close()
        self.worker_sock.close()
        for conn, _ in self.connections:
            conn.close()
        for thread in self.threads:
            thread.join()

    def rrs(self):

        processs = self.process_pool
        workers = self.workers
        total_slots_free = 0
        num = 0
        total = len(processs) 

        for worker in workers:
            num += 1
            total_slots_free += worker['slot_free_count']
        c = 0
        # empty process list?
        while total:    
            for process in processs:
                if not process['depends']:     
                    while not total_slots_free:    
                        time.sleep(1)
                        num = 0
                        for worker in workers:     
                            num += 1
                            total_slots_free += worker['slot_free_count']

                    if total_slots_free:
                        if workers[c]['slots']:
                            process['wid'] = workers[c]['worker_id']
                            workers[c]['slot_free_count'] -= 1
                            Master.send_process(process)
                            c = (c+1) % num
                            total -= 1
                else:
                    continue

    def rs(self):

        processs = self.process_pool
        workers = self.workers
        total_slots_free = 0
        num = 0
        total = len(processs)    # number of processs
        for worker in workers:
            num += 1
            total_slots_free += worker['slot_free_count']
        print("processS TO DO :\n", processs, "\n")
        print("Total number of free slots = ", total_slots_free)
        while total:
            # loop till list of processs become empty
            for process in processs:
                if (not process['depends']) and process['sta_val'] == "PENDING":   #no dependency process?
                    print("Found process with 0 dependencies..")
                    # free slots available?
                    while total_slots_free == 0:    
                        time.sleep(1)
                        num = 0
                        # no of slots?
                        for worker in workers:   
                            num += 1
                            total_slots_free += worker['slot_free_count']

                    if total_slots_free:

                        x = random.randrange(0, num)
                        if workers[x]['slots']:
                            process['wid'] = workers[x]['worker_id']
                             # worker's free slot updation
                            workers[x]['slot_free_count'] -= 1     
                            Master.send_process(process)
                            process['sta_val'] = sta_val(4)
                            total -= 1

    
    def least_loaded_scheduler(self):

        processs = self.process_pool
        workers = self.workers
        total_slots_free = 0
        total = len(processs)
        num = 0

        for worker in workers:
            num += 1
            total_slots_free += worker['slot_free_count']
        # process_mapping = []

        while total:    # until process list in empty
            for process in processs:
                if not process['depends']:         # if is a process with no dependencies
                    while total_slots_free == 0:       # check for free slots
                        time.sleep(1)
                        num = 0
                        for worker in workers:      # check for number of slots
                            num += 1
                            total_slots_free += worker['slot_free_count']

                    if total_slots_free:

                        if not process['depends']:
                            y = list(map(lambda rept: rept['slot_free_count'], workers))
                            rep = y.index(max(y))
                            if workers[rep]['slots'] > 0:
                                process['wid'] = workers[rep]['worker_id']
                                workers[rep]['slot_free_count'] -= 1
                                Master.send_process(process)
                                total_slots_free -= 1

    def listen_to_request(self):
        listen_port = 5000
        self.request_sock.bind(('localhost', listen_port))
        self.request_sock.listen(5)
        while True:
            connection, addr = self.request_sock.accept()

            while True:
                message=connection.recv(1024)
                if not len(message) == 0:
                    print("Adding processs...\n")
                    data = json.loads(message)
                    job = dict()
                    job['id'] = data['job_id']
                    job['processs'] = []
                    print("Initial process pool:\n", self.process_pool)
                    map_processs = []   # stores mapprocess ids
                    red_processs = []   # stores redprocess ids

                    for red in data['reduce_processs']:
                         red_processs.append(red['process_id'])

                    for mapper in data['map_processs']:
                        print("IN MAPPER LOOP!!!!")
                        process = dict()
                        process['wid'] = []
                        process['depends'] = []
                        process['satisfies'] = red_processs
                        process['sta_val'] = sta_val(2)
                        process['job_id'] = job['id']
                        process['process_id'] = mapper['process_id']
                        map_processs.append(mapper['process_id'])
                        process['duration'] = mapper['duration']
                        
                        self.process_pool.append(process)
                        job['processs'].append(process)
                    print("process pool after the mapper loop :\n", self.process_pool)

                    for red in data['reduce_processs']:
                        print("IN REDUCER LOOP!!!!")
                        process = dict()
                        process['wid'] = []
                        process['depends'] = map_processs
                        process['satisfies'] = []
                        process['sta_val'] = sta_val(2)
                        process['job_id'] = job['id']
                        process['process_id'] = red['process_id']
                        process['duration'] = red['duration']
                        
                        self.process_pool.append(process)
                        job['processs'].append(process)
                    print("process pool after the reducer loop :\n", self.process_pool)

                # self.job_pool.put(job)#adding job to jobpool
                # print("processpool updated")

    def listen_to_worker(self):
        print("Listening to workers now...\n")
        s = self.worker_sock

        port_to_send = 5001
        s.bind(('localhost', port_to_send))
        s.listen(5)

        while True:
            connection, addr = s.accept()
            while True:

                message = connection.recv(1024)     # dict process
                if not len(message) == 0:
                    data = message.decode()
                    process = json.loads(data)
                    # need to update process pool
                    # need to update satisfies if it is map process
                    self.update_worker_params(process)

    def update_worker_params(self, process):
        # mapper update -> it finds the processs that it satisfies and then deletes it from dependent lists,also removes from data pool
        # reducer update -> red process removed from process pool
        worker_id = process['wid']
        t = process['process_id']
        print("TRYING TO UPDATE :")
        print("process_id : ", t)
        print("worker : ", worker_id)

        red_modify = []
        if "M" in t:
            red_modify = process['satisfies']
        print("red_modify : ", red_modify)
        for tp in self.process_pool:
            print('\n')
            tp_id = tp['process_id']
            print("Current tp_id = ", tp_id)
            print(tp['depends'])
            if tp_id == t:
                self.process_pool.remove(tp)
            elif tp_id in red_modify:
                tp['depends'].remove(t)

        for worker in self.workers:
            if worker['worker_id'] == worker_id:
                worker['slot_free_count'] += 1

    def run(self):
        print("In master.run()")
        while True:

            if len(self.process_pool) < 4:
                time.sleep(1)

            elif self.scheduler == 'round robin':
                print("Using Round Robin scheduling")
                self.rrs()

            elif self.scheduler == 'random':
                print("Using random scheduler")
                self.rs()

            elif self.scheduler == 'least loaded':
                self.least_loaded_scheduler()

            else:
                continue

    def send_process(self, process):
        print("Sending process :\n", process)
        worker_id = process['wid']
        port_to_send = [worker['port'] for worker in self.workers if worker['worker_id'] == worker_id]
        print("PORT = ", port_to_send)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("localhost", port_to_send[0])) 
        message = json.dumps(process)
        s.send(message.encode())  
        s.close()
        del s


if __name__ == '__main__':
    #CMP,auto removed when done
    master = Master('config.json', 'random')
    master.run()
