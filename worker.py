import socket
import json
import random
import time
import sys
from enum import Enum
from threading import Thread
from queue import Queue

port=int(sys.argv[1])
task=sys.argv[2]
def sleep(process):
    print(process)
    print("Worker:",task,"work recieved.")
    duration=process['duration']
    #duration of sleep
    time.sleep(duration)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", 5001))
        #process sent back to worker
        message=json.dumps(process)
        s.send(message.encode())
        print("message for completion sent.")

def process_run(s):
    s.bind(('localhost',port))
    s.listen(5)
    connection,addr=s.accept()
    print("Connection accepted!")
    while(1):
        message=connection.recv(1024)
        if(len(message)!=0):
            process=message.decode()
            process=json.loads(process)
            #1 thread per process
            assigned = Thread(target=sleep, args=(process,))
            assigned.start()
if __name__ == '__main__':
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener = Thread(target=process_run, args=(s,))
    #listening to process in thread
    listener.start()
