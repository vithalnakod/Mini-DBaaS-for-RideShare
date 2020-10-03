import sqlite3
import csv
import pika
import time
import json
from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging

logging.basicConfig()
time.sleep(15)

#start the zookeeper
zk = KazooClient(hosts='zoo:2181')
zk.start()


#check if the path given is present, if not then create the path
zk.ensure_path("/slave_path")

#create a new node
zk.create("/slave_path/node",b"slave nodes",sequence=True, ephemeral=True)

"""This function is used to create the database and create all the tables required.
We also insert all the areaname and areaid in the Area table. 
"""
def createTable():
	conn= sqlite3.connect("RideShare.db")
	c= conn.cursor()

	#create user table
	query="CREATE TABLE IF NOT EXISTS User(username varchar not null,password varchar not null,primary key(username));"
	c.execute(query)

	#create table Area
	query="CREATE TABLE IF NOT EXISTS Area(areaname varchar not null,areaid integer, primary key(areaid))"
	c.execute(query)
	try:

		with open('AreaNameEnum.csv') as File:
			rows= csv.reader(File)
			i=0
			for row in rows:
				if i:
					try:
						query= "INSERT INTO Area(areaname, areaid) VALUES ('"+str(row[1])+"', '"+str(row[0])+"');"
						c.execute(query)

					except Exception as e:
						print("BECAUSE  ",e)
				i=1
	except Exception as e:
		print("CANNOT WRITE TO AREA",e)

	#create Ride Table
	query="CREATE TABLE IF NOT EXISTS Ride(rideid integer not null,username varchar not null,timestamp integer not null,\
	                                                    source integer not null,dest integer not null,\
	                                                    primary key(rideid),\
	                                                    foreign key(username) references User(username) on delete cascade,\
														foreign key(source) references Area(areaid) on delete cascade,\
														foreign key(dest) references Area(areaid) on delete cascade);"
	c.execute(query)

	#create RideUser table
	query= "CREATE TABLE IF NOT EXISTS RideUser(rideid integer not null, username varchar not null,\
													foreign key(rideid) references Ride(rideid) on delete cascade,\
													foreign key(username) references User(username) on delete cascade);"
	c.execute(query)

	#create table count
	query= "CREATE TABLE IF NOT EXISTS Count(total integer not null);"
	c.execute(query)

	conn.commit()
	conn.close()


#Establish connection with RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
channel = connection.channel()


"""This function is used so that slave is consistent with master. When a write operation is completed,
the write database function broadcasts the query. This sync functions takes the query and executes it.
On success of execution of the query we send 200 as status code and if it wasn't succesful we send 400 as status code.
"""
def sync(ch, method, properties, body):
	
	req=json.loads(body.decode())
	query=req["query"]
	res={}
	try:
		conn=sqlite3.connect("RideShare.db")
		c= conn.cursor()
		q="PRAGMA foreign_keys=OFF"
		c.execute(q)
		conn.commit()
		q="PRAGMA foreign_keys=ON"
		c.execute(q)
		conn.commit()
		try:
			c.execute(query)
			conn.commit()
			res["count"]=1
			res["status"]=200
			conn.close()
			print("WRITING TO DATABASE COMPLETED IN SYNC",res)

		except Exception as e:
			print(e)
			conn.commit()
			conn.close()
		
	except Exception as err:
		print(err)


"""This function is used to write all the db write operations when a new slave is created.
persistent_queue stores all the db write operations. we execute all the queries from the persistent_queue.
auto_ack is set to false so that the message wont be deleted from the queue, since we need the message if
another slave is created. we call the sync function which executes the query."""
def all_db_operations(channel):
	
	declare = channel.queue_declare(queue="persistent_queue", durable=True)

	try:
		noOfMsg=declare.method.message_count
		while(noOfMsg>0):
	
			messageRes = channel.basic_get(queue='persistent_queue',auto_ack=False)
			print(messageRes,"\nMessage got from queue")
			empty_str=''
			sync(empty_str,empty_str,empty_str,messageRes[2])
			noOfMsg=noOfMsg-1
            
	except Exception as e:
		print(e,"errorrr")
		
	print("slave and master consistent with each other")
	channel.close()


"""This function is used to read database and send the response.
First we create final columns from the columns list. There might be more than one column,
so its necessary to take all the columns and convert it into string. Same way we create 
final condition. Then we execute the query, if not successful then return 400 as status code.
Then we convert the output in required format. For example when we execute a query, we get the output as list of lists,
but we need all users under one list (for example) etc. Then we return the result with 200 as status code as a message to the response queue.
"""
def read_database(ch,method,props,body):
	req= json.loads(body.decode())
	table= req["table"]
	columns= req["columns"]
	where= req["where"]
	final_column=''
	for i in columns:
		j=str(i)
		final_column+= j+', '
	
	final_column=final_column[:-2]
	query=''
	if len(where)==0:
		query="SELECT "+final_column+" FROM '"+table+"';"
	else:
		final_where=''
		for i in where:
			final_where+= i+' and '
		final_where= final_where[:-5]
		query='SELECT '+final_column+' FROM '+table+' WHERE '+ final_where+';'
	res={}
	print(query)

	try:
		conn=sqlite3.connect("RideShare.db")
		c= conn.cursor()
		check= c.execute(query).fetchall()

		count = len(check)
    
		res['count']=count
		column_index={}

		for i in range(len(columns)):
			column_index[i]=columns[i]
    
		for i in columns:
			res[i]=[]

		for i in range(count):
			for j in range(len(check[i])):
				res[column_index[j]].append(check[i][j])
		
		res['status']=200
		print(res)
		conn.commit()
		conn.close()

	except Exception as err:
		print(err)
       
    
	ch.basic_publish(exchange='', 
		routing_key=props.reply_to, 
    		properties=pika.BasicProperties(correlation_id=props.correlation_id),
		body=json.dumps(res))

	ch.basic_ack(delivery_tag=method.delivery_tag)


"""This function is used to write to the database. flag variable is used
to indicate if the query will be related to insert, delete or update.
If the value of flag is 0 then its related to insert, if its 1, then related to delete,
if its 2 then its related to update. We convert the columns and values in required format.
We then execute the query. if the query execution was unsuccessful we return 400.
We broadcast this query so that the slave is consistent with master. We also write this query 
to our persistent queue so that whenever a new slave is created all the write operations are executed again.
"""
def write_database(ch,method,props,body):
	req = json.loads(body.decode())
	table=req["table"]
	flag=req["flag"]
	query=''
	response_var=0
	if flag==0:     #INSERT
		values=req["values"]
		columns=req["columns"]
		final_column=''
		for i in columns:
			final_column+="'"+i+"'"+', '
		final_column=final_column[:-2]
		final_values=''
		for i in values:
			final_values+="'"+i+"'"+', '
		final_values=final_values[:-2]
		query='INSERT INTO '+table+' ('+final_column+') VALUES ('+final_values+');'

	elif flag==2:              #update
		columns= req["columns"]
		sett= req["sett"]
		final_columns=''
		for i in columns:
			final_columns+="'"+ i+"', "
		
		final_columns=final_columns[:-2]
		
		query='UPDATE '+table+' SET '
		for i in sett:
			query += columns[i]+" = "+sett[i]+", "
		
		query=query[:-2]
		query+= ";"
	
	else:
		cond=req["condition"]
		if len(cond)==0:
			query='DELETE FROM '+table+' ;'
		else:
			final_cond=''
			for i in cond:
				final_cond+=i+' and '
			
			final_cond=final_cond[:-5]
			
			query='DELETE FROM '+table+' WHERE '+final_cond+";"
	
	print(query)
	res={}
	try:
		conn=sqlite3.connect("RideShare.db")
		c= conn.cursor()
		q="PRAGMA foreign_keys=OFF"
		c.execute(q)
		conn.commit()
		q="PRAGMA foreign_keys=ON"
		c.execute(q)
		conn.commit()

		try:
			c.execute(query)
			conn.commit()
			res["count"]=1
			res["status"]=200
			conn.close()
			response_var=1
		
			sync={"query":query}

			channel.exchange_declare(exchange='logs', exchange_type='fanout')
			
			channel.basic_publish(exchange='logs', routing_key='', body=(json.dumps(sync)))

			channel.basic_publish(exchange='',routing_key='persistent_queue',body=(json.dumps(sync)),properties=pika.BasicProperties(delivery_mode=2,))
        
		except Exception as e:
			print(e)
			conn.commit()
			conn.close()
		
	except Exception as err:
		print(err)

	ch.basic_publish(exchange='', 
		routing_key=props.reply_to, 
    		properties=pika.BasicProperties(correlation_id=props.correlation_id),
		body=json.dumps(res))
	ch.basic_ack(delivery_tag=method.delivery_tag)


#master variable is set to true which indicates that this is a master
master=True


#calling the createTable() function which creates the database if its not present
createTable()


#we call the write database function and create the necessary queues if not present
if(master):
	#print("I AM THE MASTER")
	channel.queue_declare(queue='write_queue')
	channel.basic_qos(prefetch_count=1)
	channel.basic_consume(queue='write_queue', on_message_callback=write_database)

	channel.start_consuming()


"""we call the read database function and create the necessary queues if not present
we call all_db_operations function first, so that all the previous write operations are executed,
on_message_callback=sync is used so that whenever we read a message, the sync function is called 
so that slave is consistent with master
"""
elif(not master):
	#print("i am the slave")
	sync_channel= connection.channel()
	all_db_operations(sync_channel)
	channel.queue_declare(queue='read_queue')
	channel.exchange_declare(exchange='logs', exchange_type='fanout')
	result = channel.queue_declare(queue='', exclusive=True)
	queue_name = result.method.queue
	channel.queue_bind(exchange='logs', queue=queue_name)
	channel.basic_consume(queue=queue_name, on_message_callback=sync, auto_ack=True)
	channel.basic_qos(prefetch_count=1)
	channel.basic_consume(queue='read_queue', on_message_callback=read_database)
	channel.start_consuming()	
