import math
import matplotlib.pyplot as plt

def get_results(filename):

	task_recvd_time = dict()
	task_sent_time = dict()
	tasks_every_worker=dict()
	tasks_diff_time = dict()
	tasks_end_time = dict()
	with open("logs.txt","r") as log :
		for l in log.readlines() :
			l = l.strip("\n")
			l = l.split(" ")
			task_recvd_time[l[0]] = float(l[1])
	#print("task_recvd_time ",task_recvd_time)

	#print("Results for ",filename.split('.')[0])

	tasks_time=[]
	jobs_time=[]
	with open(filename,'r') as f:
		lines = f.readlines()	
	
	for l in lines:
		l = l.strip('\n')
		l = l.split()
		
		if 'Scheduler' in l:
			continue
	
		if 'R' in l[0] or 'M' in l[0]:
			tasks_time.append(float(l[-1]))
			task_sent_time[l[0]] = float(l[-3])
			tasks_end_time[l[0]] = float(l[-2])
			tasks_diff_time[l[0]] = task_recvd_time[l[0]]  - float(l[-3])
			if int(l[1]) in tasks_every_worker :
				tasks_every_worker[int(l[1])].append(l[0])
			else:
				tasks_every_worker[int(l[1])] = [l[0]]
				
		
		if 'Job' in l :
			jobs_time.append(float(l[2]))
			
	tasks_time = sorted(tasks_time)
	jobs_time = sorted(jobs_time)
	
	#print(tasks_every_worker)
	
	for w,tasks in tasks_every_worker.items() :
		plt.title(f"Worker {w}'s analysis on {filename.split('.')[0]}")
		ts = sorted([task_recvd_time[i] for i in tasks])
		p_list=[]
		for t in ts:
			cnt=0
			for task in tasks:
				if tasks_end_time[task] > t  and  task_sent_time[task] < t:
					cnt+=1
			p_list.append(cnt)
			cnt=0
		plt.plot(range(len(ts)),p_list)
		plt.xlabel("Time steps")
		plt.ylabel("Running Tasks")
		plt.savefig(f"Worker {w}-{filename.split('.')[0]}")
		plt.show()
	
	

	average_time_jobs = sum(jobs_time)/len(jobs_time)
	print(f"Average Job Time {average_time_jobs}s")
	average_time_tasks = sum(tasks_time)/len(tasks_time)
	print(f"Average Tasks Time {average_time_tasks}s")
	median_time_jobs=None
	median_time_tasks=None
	if len(tasks_time)%2==0 :
		median_time_tasks = (tasks_time[len(tasks_time)//2] + tasks_time[(len(tasks_time) - 1)//2])/2
	else:
		median_time_tasks = tasks_time[len(tasks_time)//2]
		
		
	if len(jobs_time)%2==0 :
		median_time_jobs = (jobs_time[len(jobs_time)//2] + jobs_time[(len(jobs_time)-1)//2])/2 
	
	else:
		median_time_jobs = jobs_time[len(jobs_time)//2]
		
	print(f"Median Job Time {median_time_jobs}s")
	print(f"Median Tasks Time {median_time_tasks}s")
	
	epochs=range(1,len(tasks_time)+1)
	plt.plot(epochs,tasks_time)
	plt.xlabel("Tasks")
	plt.ylabel("Time in seconds")
	plt.title(filename.split('.')[0] + " Scheduler")
	
	plt.show()
	
	
	
file='LL.txt'
get_results(file)
	
