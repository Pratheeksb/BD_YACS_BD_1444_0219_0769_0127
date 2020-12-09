import sys
import time
import threading
from queue import Queue
import random
import concurrent.futures
import json
import socket
list_of_wnodes=None
job_port = 4000
dtask = {}
conf = sys.argv[1]
with open(conf,'r') as config:
	list_of_wnodes = json.loads(config.read())['workers']

#tracks the number of free slots available for easy allocation
for node in range(len(list_of_wnodes)):
	list_of_wnodes[node]['free_slots']=list_of_wnodes[node]['slots']
	list_of_wnodes[node]['id']=node

#recieves job requests by binding to port 5000.
master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
	master_socket.bind(('localhost',5000))
	print(f"Master listening on port 5000 for job requests...")
except Exception as e:
	print(f"[Error : {e}]unable to connect to port 5000")


#read scheduling algorithm opted from user end.
scheduler = sys.argv[2]
Round_Robin_prev = None

#connect worker sockets to port after creation for assigning tasks and handle the exception in case connection to port fails.
socket_worker = []
for node in range(len(list_of_wnodes)):
	soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	try:
		soc.connect(('localhost', int(list_of_wnodes[node]['port'])))
		socket_worker.append(soc)
	except Exception as e:
		print(f"[Error : {e}] Couldn't connect to port {list_of_wnodes[node]['port']}")
    
#listen to connection requests.
master_socket.listen(1)

#listen to port 5001 for updates on task completion by workers.
update_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
	update_socket.bind(('localhost',5001))
	print(f"Listening to port 5001 for task updates...")
	update_socket.listen(len(list_of_wnodes))

except Exception as e:
	print(f"[Error : {e}] unable to connect to port 5001")
	

#to prevent race conditions, lock is introduced. Some variables defined to store tasks list, map and reduce.
lock = threading.Lock()
task_queue = Queue()
task_dict = dict()
_map = []
_reduce = []
_maparray = []

def RRSch():
	"""
	During First run, first available worker with free slots is taken for the task.
	The previous worker is stored. Using the round robin method, we check for free slots in worker and task is assigned.
	Finally Round_Robin_prev which stores previous worker is updated with current worker and hence, the algorithm continues from the next worker during next iteration.
	"""
	global Round_Robin_prev
	worker_sch = -1
	selected = False
	end=-1
	j=0
	if Round_Robin_prev==None:
		while (j < len(list_of_wnodes)) and not selected :
			if list_of_wnodes[j]['free_slots']>0:
				worker_sch = j
				selected=True
			elif not selected:
				j+=1
	else:
		j=(Round_Robin_prev+1)%len(list_of_wnodes)
		end = Round_Robin_prev
		while (j!=end) and not selected:
			if list_of_wnodes[j]['free_slots']>0:
				worker_sch= j
				selected=True
			j=(j+1)%len(list_of_wnodes)

	if worker_sch!=-1:
		Round_Robin_prev = worker_sch
	
	return worker_sch


def LLSch():

	"""
 	This algorithm searches for the maximum number of free slots available. 
	 If all workers are busy, the algorithm keeps searching until the max free slots has been found.
	
	"""
	worker_sch = -1
	selected=False
	while not selected:
		worker_sch = 0
		for i in range(1,len(list_of_wnodes)):
			if list_of_wnodes[worker_sch]['free_slots'] < list_of_wnodes[i]['free_slots']:
				worker_sch = i
		#checks for busy workers    
		if list_of_wnodes[worker_sch]['free_slots']<1:
			time.sleep(1)
		else:
			selected = True
            
	return worker_sch
    


def RandSch():

	"""
	This algorithm randomly selects a worker with free slots and keeps searching until requirements of free slots are met.
	"""
	selected=False
	worker_sch = -1
	random_list = list(range(len(list_of_wnodes)))
	rand = random.choice(random_list)
	checked_nodes = [rand]
	while (len(checked_nodes) < len(list_of_wnodes) and list_of_wnodes[rand]['free_slots'] <= 0) and not selected:
		rand = random.choice(random_list)
		while rand in checked_nodes:
			checked_nodes.append(rand)
			rand = random.choice(random_list)
		if list_of_wnodes[rand]['free_slots']:
			worker_sch = rand
			selected=True
			
	if not selected and list_of_wnodes[rand]['free_slots'] :
		worker_sch=rand
		selected=True
		
	return worker_sch


def choose_worker(scheduler_type):

	"""
	Since free slots are shared resources allocating a slot for a task must be handled
	correctly and that is done using lock from the threading library
	Finally selected worker is returned
	"""
    
	worker_sch = -1
	lock.acquire()
	if scheduler_type.upper() =='RANDOM':
		worker_sch = RandSch()
				
	elif scheduler_type.upper()=='RR':
		worker_sch = RRSch()
	
	elif scheduler_type.upper()=='LL' :
		worker_sch = LLSch()
	
	lock.release()
	return worker_sch




def mapper_scheduler(scheduler_type):

	"""
	Firstly select a worker according to the scheduling algorithm, if workers are
	busy then the parent function is asked to wait for 1 second. Otherwise, for every
	tasks in the _map variable , check if the task is not submitted , if yes
	assign the worker with that task and return the json object to parent function
	"""
	chosen_worker = choose_worker(scheduler_type)
	if chosen_worker == -1:
		return 0
	else:
		for j in range(len(_map)):
			if(_map[j]['submitted'] == 0):
				list_of_wnodes[chosen_worker]['free_slots']-=1
				_map[j]['worker_id'] = list_of_wnodes[chosen_worker]['worker_id']
				_map[j]['id'] = chosen_worker
				_map[j]['submitted'] = 1
				return json.dumps(_map[j])
	return 1



def reducer_scheduler(scheduler_type):

	"""
	select the worker , if the worker isn't available then ask the parent function
	to wait for 1 second. Otherwise for every reduce task if it's not submitted
	and all the map tasks of respective job are completed then the selected worker
	is assigned with the task and the json object is returned to parent function
	"""

	selected_worker_id = choose_worker(scheduler_type)
	if selected_worker_id == -1 :
		return 0
	else:
		for j in range(len(_reduce)):
			if int(_reduce[j]['submitted'])==0:
				reducer_id = _reduce[j]['task_id'].split('_')[0]
				for job in _maparray:
					temp_set = set(job[1:])
					if len(temp_set)==1 and list(temp_set)[0]==1 and int(job[0])==int(reducer_id):
						list_of_wnodes[selected_worker_id]['free_slots']-=1
						_reduce[j]['worker_id'] = list_of_wnodes[selected_worker_id]['worker_id']
						_reduce[j]['id'] = selected_worker_id
						_reduce[j]['submitted'] = 1
						return json.dumps(_reduce[j])
		return 1

				
def get_job_requests(job_q,task_dict):
	while True:
		conn, addr = master_socket.accept()
		jobs = conn.recv(10000)
		if jobs:
			jobs = json.loads(jobs)
			job_q.put(jobs)
			print("Job ", jobs['job_id'], "received")
			#print(task_dict)
			task_dict[int(jobs['job_id'])] = [time.time(),len(jobs['reduce_tasks'])]



def assign_job_to_worker(job_queue,task_dict):
	scheduler_type = scheduler
	code=None
	while True:
		while not job_queue.empty():
			job = job_queue.get()
			temp_arr = [job['job_id']]
			for i in range(len(job['map_tasks'])):
				temp_arr.append(0)
			_maparray.append(temp_arr)
			for task in job['map_tasks']:
				task['completed'] = 0
				task['submitted'] = 0
				_map.append(task)
                
			for task in job['reduce_tasks']:
				task['completed'] = 0
				task['submitted'] = 0
				_reduce.append(task)

		code = mapper_scheduler(scheduler_type)
		if code == 0:
			time.sleep(1)
		else:
			while code != 1:
				if code == 0 :
					time.sleep(1)
				else:
					code = json.loads(code)
					dtask[code['task_id']] = time.time()
					socket_worker[code['id']].send(json.dumps(code).encode())
			
				code = mapper_scheduler(scheduler_type)

def job_update(job_q,task_dict):

	scheduler_type=scheduler
	result = ''
	outputfilename = scheduler_type+".txt"
	with open(outputfilename,"w") as f:
		f.write("Scheduler used : " +scheduler_type + "\n")
	
	while True:
		conn, addr = update_socket.accept()
		update_task = conn.recv(16384)
		if update_task:
			update_task = json.loads(update_task)
			#print("Worker id : ",update_task['id'])
			x = dtask[update_task['task_id']]
			dtask[update_task['task_id']]= time.time()-x
			with open(outputfilename,"a") as f:
				f.write(str(update_task['task_id']) + " ")
				f.write(str(int(update_task['id'])+1) + " ")
				f.write(str(x) + " ")
				f.write(str(time.time()) + " ")
				f.write(str(dtask[update_task['task_id']]) + "\n")
            
			print(str(update_task['task_id']) + ":")
			print(str(dtask[update_task['task_id']]) + "\n")
            #jobs
			jid = update_task['task_id'].split("_")[0]
			if(update_task['task_id'].split("_")[1][0]=='R'):
				task_dict[int(jid)][1]-=1
				if(task_dict[int(jid)][1]==0):
					t = task_dict[int(jid)][0]
					t = time.time() - t
					print("Job : "+str(jid) + " ")
					print(str(t) + "\n")
					with open(outputfilename,"a") as f:
						f.write("Job "+"["+str(jid)+']' + " ")
						f.write(str(t) + "\n")

			for i in range(len(_map)):
				if _map[i]['task_id'] == update_task['task_id']:
					_map[i]['completed']=1
			
			task_id = update_task['task_id'].split('_')
			if(task_id[1][0]=='M'):
				task_num = int(task_id[1][1:])
				for i in range(len(_maparray)):
					if(_maparray[i] != None and int(_maparray[i][0])==int(task_id[0])):
						_maparray[i][task_num+1]=1
			list_of_wnodes[int(update_task['id'])]['free_slots']+=1

		result = reducer_scheduler(scheduler_type)
		if(result==0):
			time.sleep(1)
		else:
			while result != 1:
				if(result == 0):
					time.sleep(1)
				else:
					result = json.loads(result)
					dtask[result['task_id']] = time.time()
					socket_worker[result['id']].send(json.dumps(result).encode())
				result = reducer_scheduler(scheduler_type)




task_queue = Queue()
task_dict = dict()
main_thread = threading.Thread(target = get_job_requests,args=(task_queue,task_dict))
job_assign_thread = threading.Thread(target = assign_job_to_worker, args=(task_queue,task_dict))
job_update_thread = threading.Thread(target = job_update, args=(task_queue,task_dict))

main_thread.start()
job_assign_thread.start()
job_update_thread.start()

