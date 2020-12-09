import json
import socket
import time
from threading import Thread
import sys
import concurrent.futures

port = int(sys.argv[1])
worker_id = int(sys.argv[2])
worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
	worker_socket.bind(('localhost', port))
	worker_socket.listen(1)
	print(f'Worker node {worker_id} listening on port:', port)

except Exception as e:
	print(f"[Error : {e} ] couldn't connect on port {port}")


def receive_tasks(task_queue):
	"""
	Receive tasks from the master on port accepted as a command line argument and send 
	an acknowledgement if task is received.
	"""
	connection, addr = worker_socket.accept()
	if connection:
		while True:
			data = connection.recv(1024)
			if data:
				task = json.loads(data.decode())
				with open("logs.txt", 'a') as log:
					log.write(task['task_id'] + " " + str(time.time()) + "\n")
				task_queue.append(task)
				connection.send((task['task_id']+' is received').encode())
			

def simulate_task(task_queue):

	"""
	Check if the queue of the tasks is empty. If yes, then wait for 1 second and repeat the execution
	within while loop. If the queue is not empty then dequeue the task and simulate python
	it, after completion using send_update_to_master function update to master that the
	task has been completed is sent
	"""
	while True:
		if not task_queue:
			time.sleep(1)
		else:
			while task_queue:
				task = task_queue.pop(0)
				time.sleep(task['duration'])
				task['duration'] = 0
				# send_update_to_master(task)
				with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as notify_master_socket:
					try:
						notify_master_socket.connect(('localhost', 5001))
						notify_master_socket.send(json.dumps(task).encode())
						print(task['task_id'], ' completed successfully')
			
					except Exception as ef:
						print(f"[Error : {ef} ]: Couldn't send the update!")


task_queues = []
task_requests = Thread(target=receive_tasks, args=(task_queues,))
task_updates = Thread(target=simulate_task, args=(task_queues,))

threads = [task_requests, task_updates]

for thread in threads:
	thread.start()

for thread in threads:
	thread.join()

