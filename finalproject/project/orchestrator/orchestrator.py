import sqlite3 
from flask import Flask, render_template,jsonify,request,abort,Response 
import requests 
import json 
import csv
import pika
import time 
import uuid
import docker
from datetime import datetime
from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging
import threading
import math
lock = threading.Lock()
logging.basicConfig()
time.sleep(15)
app=Flask(__name__)

#setting wait seconds equal to 2 mins
WAIT_SECONDS= 120

#using a global variable crash_slave which is used when ChildrenWatch is triggered
crash_slave=0

zk = KazooClient(hosts='zoo:2181')
zk.start()

#ensure path /slave_path,create if not present
zk.ensure_path("/slave_path")

"""create a file called count.txt and write two lines.
first line, tells us about the number of http requests made to read database in 2 mins.
second line, tells us if the read database is called for the first time
second line will be a integer which takes values of 0 and 1
0 meaning, the request to read database is called for the first time
1 meaning, the read database was called before.
first line count will be reset to 0 every two lines."""

file=open("count.txt","w")
file.write("0")
file.write("\n")
file.write("0")

file.close()


"""This function is called to increment the count of http request to read database.
first open the file count.txt read the first line and store it in count_line variable
increment the count and write it to the same file.
Here we are writing two lines, one to denote the number of requests and other to say if read database was called before.
"""
def modify_count():
    
    file=open("count.txt","r")
    count_line=file.readline()
    count=int(count_line)+1
    file.close()

    file=open("count.txt","w")
    file.write(str(count))
    file.write("\n")
    file.write("1")

    file.close()
    

"""This function is called to crash the slave with highest Pid.
We use this function when we want to scale in.
If the number of slaves requires is less than the number of slaves running, then this function
is called which will stop the slave with highest Pid.
First we append all containers with ancestor:"project_slve", and append the Pid along with their id into a list.
Then we sort the list and stop the slave with the highest Pid.
We are not using the crash api because the @ChildrenWatch will be triggered whenever we crash a api.
In the crash api we have modified the global variable crash_slave so that a new slave is created
when we call the crash api, and since we don't want a new slave to be created when we scale in, we are using this function

"""
def crash_through_scale():
    
    client= docker.from_env()

    res=client.containers.list(filters={"ancestor":"project_slave"})
    api_c=docker.APIClient()
    all_c=[]

    for i in range(len(res)):
           
        all_c.append([(api_c.inspect_container(res[i].id))['State']['Pid'],res[i].id])
    
    all_c.sort()
    
    if len(all_c)==0:
        #print("NO CONTAINERS")
        return 

    large_pid =all_c[len(all_c)-1][0]
    to_be_killed = all_c[len(all_c)-1][1]


    for i in range(len(res)):
        if res[i].id==to_be_killed:
            res[i].stop()
            res[i].remove()
            break


"""This function reads the file count.txt and stores the count of http requests.
Then we call the list_all_workers API which returns us the list of all workers (master and slave).
We store the number of slaves running in container_count variable. 
We use math.ceil(count/20) to scale out or scale in depending on the number of read requests made in last 2 mins.
new_containers variable stores how many slaves we need at that time and containers_count variable says how many slaves we have.
If the number of slaves running is greater than the number of slaves required then we stop the additional slaves using the 
crash_through_scale().
If the number of slaves running is less than the number of slaves required, we run the additional slaves through client.containers.run()
This function runs itself every 2 mins. 
"""
def scaling_func():

    client=docker.from_env()
    file=open("count.txt","r")
    count=file.readline()
    count=int(count)
    count_deamon= file.readline()
    count_deamon= int(count_deamon)
    file.close()

    res=requests.get("http://0.0.0.0:5000/api/v1/worker/list")
    
    container_count=len(json.loads(res.text))-1
    
    new_containers=math.ceil(count/20)
    if(new_containers==0):
        new_containers=1

    if(new_containers>container_count):
        scale_out=new_containers-container_count
        for i in range(scale_out):
            client.containers.run("project_slave",command="python slave.py",network="project_default",detach=True)
    
    else:
        scale_in=container_count-new_containers
        for i in range(scale_in):
            crash_through_scale()

    file=open("count.txt","w")
    if count_deamon==0:
        file.write("1")
        file.write("\n")
        file.write("1")

    else:
        file.write("0")
        file.write("\n")
        file.write("1")

    file.close()
    threading.Timer(WAIT_SECONDS, scaling_func).start()


"""First establish connection with RabbitMQ server. Then declare the necessary queues"""
class RpcClient(object):

    def __init__(self,call_back_queue_name=''):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rmq'))

        self.channel = self.connection.channel()
        self.call_back_queue_name = call_back_queue_name
        if self.call_back_queue_name:
            result = self.channel.queue_declare(queue=self.call_back_queue_name)
        else:
            result = self.channel.queue_declare(queue=self.call_back_queue_name,exclusive=True)
        
        #since we gave a null string while declaring the queue, the rmq automatically assigns a name, to fetch that name we use result.method.queue
        self.callback_queue = result.method.queue

        #on_message_callback is used to call a function whenever it receives a message
        #this is used to read the messages from the queue which is specified in queue parameter
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    """This function is used so that the response received is exactly for the request sent.
    This is done by using the correlation_id which is unique for all the requests."""
    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = json.loads(body.decode())

    """First this function sets a unique correlation_id to the request message.
    then it publishes the message with the some properties such as the query,
    correlation_id etc. we return the response message received"""
    def call(self,queue_name,qry):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id),
            body=json.dumps(qry))

        while self.response is None:
            self.connection.process_data_events()

        requeued_messages = self.channel.cancel()
        self.connection.close()
        return self.response


"""First we initialize the object with queue name as write_queue.
Then we send the request and call the 'call' function,
which will write the message to write_queue and get the response from response queue
"""
@app.route("/api/v1/db/write",methods=["POST"])
def write_database():
	queue='write_queue'
	write_content=request.get_json()
	obj=RpcClient()
	response = obj.call(queue,write_content)
	
	if response:
		return json.dumps(response),200
	else:
		return json.dumps(response),400
	

"""First we read the count.txt file. if the second line of this file is set to 0,
then it means that this read API is called for the first time and hence we start the 
scaling_func function. Then we increment the count of number of read requests made
by using the modify_count() function. Then we initialise the object with queue name 
as read_queue. Then the call function of this object is called whcih is responsible for
writing the request to queues and getting the response from the response queue.
"""
@app.route("/api/v1/db/read",methods=["POST"])
def read_database():
    file=open("count.txt","r")
    count=file.readline()

    call_deamon=file.readline()
    call_deamon=int(call_deamon)

    file.close()
    if(call_deamon==0):
        scaling_func()
    
    modify_count()

    queue='read_queue'
    read_content=request.get_json()
  
    read_obj=RpcClient()
    response=read_obj.call(queue,read_content)
    
    return jsonify(response),200


"""This API returns the list of all workers in ascending order of their Pid.
We use client.containers.list() which returns a list of all containers running.
We filter this list since orchestrator, rmq, zoo aren't the workers. 
We append the Pid of the workers to a new list.
Then we sort this list and return it.
"""
@app.route("/api/v1/worker/list",methods=["GET"])
def all_worker():

    client= docker.from_env()

    res=client.containers.list()
    api_c= docker.APIClient()

    ret_res=[]
    
    for i in range(len(res)):
        if res[i].name=='project_orchestrator_1' or res[i].name=='project_rmq_1' or res[i].name=='project_zoo_1':
            pass

        else:
            ret_res.append(api_c.inspect_container(res[i].id)['State']['Pid'])

    ret_res.sort()
    return json.dumps(ret_res),200


"""This function is triggered whenever a new container is created or when a container is deleted.
But we need a new slave only when a slave is crashed by calling the crash slave API.
That is the reason we have used the global variable crash_slave which when equal to 1 indicates that
a slave is stopped through the crash slave API and hence a new slave has to be created."""
@zk.ChildrenWatch("/slave_path")
def zookeep(children):
    global crash_slave
    
    if crash_slave==1:
        client= docker.from_env()
      
        client.containers.run(image="project_slave",command="python slave.py",network="project_default",detach=True)
        crash_slave=0
    
    else:
        return

"""This API is used to crash a slave with highest Pid. It returns the Pid of the slave which was killed. 
client.containers.list() is used to list out all the slaves. We are creating a list of Pid of slaves along with their id.
Then we sort the list, and large_pid variable stores the Pid of the slave to be killed. We loop through the list of all 
slaves and stop when the id matches with the id to be killed. The global variable crash_slave is set to 1 so that
after the slave is stopped, the @ChildrenWatch is triggered and a new slave is created.
"""
@app.route("/api/v1/crash/slave",methods=["POST"])
def crash():
    global crash_slave
    
    client= docker.from_env()

    res=client.containers.list(filters={"ancestor":"project_slave"})
    api_c=docker.APIClient()
 
    all_c=[]

    for i in range(len(res)):
        all_c.append([(api_c.inspect_container(res[i].id))['State']['Pid'],res[i].id])
    
    all_c.sort()
    
    if len(all_c)==0:
        return json.dumps({}),204

    large_pid =all_c[len(all_c)-1][0]
    to_be_killed = all_c[len(all_c)-1][1]
    
    killed=False

    for i in range(len(res)):
        if res[i].id==to_be_killed:
            crash_slave=1
            res[i].stop()
            res[i].remove()
            
            killed=True
            break
    
    
    if killed==True:
        
        return json.dumps(large_pid),200

    else:
        return json.dumps({}),400


"""This API is used to clear the database and return status code 200 on success.
This API first sends a write request to write database, with flag set to 1. 
Here flag=1 indicates that the query is related to delete. If the condition
is an empty list, then the query would be "DELETE FROM User" for table="User"
Similarly we clear the database by clearing all the tables and returning appropriate status code.
"""
@app.route('/api/v1/db/clear',methods=['POST'])
def cleardb():

    r1=requests.post('http://ec2-18-215-10-194.compute-1.amazonaws.com:80/api/v1/db/write',
    json={
        "table":"User",
        "flag":1,
        "condition":[]
    })

    count2= r1.json().get("count")
    if count2>0:
        r2=requests.post('http://ec2-18-215-10-194.compute-1.amazonaws.com:80/api/v1/db/write',
        json={
            "table":"Ride",
            "flag":1,
            "condition":[]
        })

        count3= r2.json().get("count")
        if count3>0:
            r=requests.post('http://ec2-18-215-10-194.compute-1.amazonaws.com:80/api/v1/db/write',
            json={
                "table":"RideUser",
                "flag":1,
                "condition":[]
            })

            count=r.json().get("count")
            if count>0:
                #print("DB CLEARED")
                return json.dumps({}),200

            else:
                return json.dumps({}),200
        else:
            return json.dumps({}),400
    else:
        return json.dumps({}),400


if __name__ == '__main__':
	app.debug=True
	app.run(host='0.0.0.0',port=5000)
