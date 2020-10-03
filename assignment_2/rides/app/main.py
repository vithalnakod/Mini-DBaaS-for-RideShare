from flask import Flask, render_template,\
jsonify,request,abort, Response
import json
import requests
from datetime import datetime
app=Flask(__name__)

import sqlite3


"""This function is used to read database and send the response.
First we create final columns from the columns list. There might be more than one column,
so its necessary to take all the columns and convert it into string. Same way we create 
final condition. Then we execute the query, if not successful then return 400 as status code.
Then we convert the output in required format. For example when we execute a query, we get the output as list of lists,
but we need all users under one list (for example) etc. Then we return the result with 200 as status code.
"""
@app.route('/api/v1/db/read',methods=['POST'])
def read_db():
    req=request.get_json()

    table= req.get("table")
    columns= req.get("columns")
    where= req.get("where")
	
		
    final_column=''
    for i in columns:
        final_column+= i+', '

    final_column=final_column[:-2]
	
    query=''
    if len(where)==0:
        query="SELECT "+final_column+" FROM "+table+";"

    else:
        final_where=''
        for i in where:
            final_where+= i+' and '
		
        final_where= final_where[:-5]
        query='SELECT '+final_column+' FROM '+table+' WHERE '+ final_where+';'
    res={}
    print(query)
    try:
        conn=sqlite3.connect("ride.db")
        c= conn.cursor()

        check= c.execute(query).fetchall()
        #print(check)
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
        conn.close()
        return json.dumps(res)
    
    except Exception as err:

        print(err)
        res['status']=400
        conn.close()
        return json.dumps(res)
    
    finally :
        if conn:
            conn.close()
        else:
            pass


"""This function is used to write to the database. flag variable is used
to indicate if the query will be related to insert, delete or update.
If the value of flag is 0 then its related to insert, if its 1, then related to delete,
if its 2 then its related to update. We convert the columns and values in required format.
We then execute the query. if the query execution was unsuccessful we return 400,
else return 200 as status code
"""
@app.route('/api/v1/db/write',methods=['POST'])
def write_db():
    req=request.get_json()
    table=req.get("table")
    flag=req.get("flag")
    query=''
    if flag==0:     #INSERT
        values=req.get("values")
        columns=req.get("columns")
        final_column=''
        for i in columns:
            final_column+="'"+i+"'"+', '
        final_column=final_column[:-2]
        final_values=''
        for i in values:
            final_values+="'"+i+"'"+', '
        final_values=final_values[:-2]

        query='INSERT INTO '+table+' ('+final_column+') VALUES ('+final_values+');'
    else:
        cond=req.get("condition")
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
        conn=sqlite3.connect("ride.db")
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
            return json.dumps(res)
        except Exception as e:
            print("HERE1")
            print(e)
            res["count"]=0
            res["status"]=400
            conn.close()
            return json.dumps(res)

    except Exception as err:
        print("HERE")
        print(err)
        res["count"]=0
        res["status"]=400
        conn.close()
        return json.dumps(res)
        
    finally :
        if conn:
            conn.close()
        else:
            pass


"""This API is used to create a ride. The details of who created, source, destination
timestamp will be extracted from the request. First we check if source and destination are
equal. If they are equal then return status code of 400 as source and destination cannot be same.
Then we check if the user exists by reading the users table. We get a list of users.
If the user doesn't exist return 400. Then we check if the given source and destination are valid.
If any one of them is invalid return 400 as status code. If both are valid then we call the write database 
API with flag set to 0 whcih indicates the query will be related to Insert. the final query will be
INSERT INTO Ride(source, destination, timestamp,username) values(source,dest,timestamp,uname).
If this query is executed successfully then return 201 created """
@app.route('/api/v1/rides',methods=['POST'])
def ADD_ride():
    req= request.get_json()
    uname= req.get("created_by")
    timestamp=req.get("timestamp")
    source=int(req.get("source"))
    dest=int(req.get("destination"))
    
    if source==dest:
        return json.dumps({}),400
    
    r=requests.get('http://ec2-3-222-255-47.compute-1.amazonaws.com:8080/api/v1/users').json()
    
    if(uname in r):
        r1=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8000/api/v1/db/read',
        json={
            'table':'Area',
            'columns':['areaname'],
            'where':["areaid="+str(source)+""]
        })
        
        count1=r1.json().get("count")
        if count1>0:
            r2=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8000/api/v1/db/read',
            json={
                'table':'Area',
                'columns':['areaname'],
                'where':["areaid="+str(dest)+""]
            })
            count2=r2.json().get("count")
            if count2>0:
                r3=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8000/api/v1/db/write',
                json={
                    'table':'Ride',
                    'flag':0,
                    'columns':['source','dest','timestamp','username'],
                    'values':[str(source),str(dest),str(timestamp),uname]
                })

                count3=r3.json().get("count")
                status_code=r3.json().get("status")
                if count3==1:
                   
                    return json.dumps({}),201

                else:
                  
                    return json.dumps({}),400

            else:
                #return dest doesn't exist
                return json.dumps({}),400
        
        else:
            #return source doesn't exist
            return json.dumps({}),400
    
    else:
        #return username doesn't exist
        return json.dumps({}),400


"""This API is used to get the list of upcoming rides with the given source and destination.
For this, we first check if the given source and destinaiton is valid by reading the Area table of the database.
If any of the two is invalid then we return the status code of 400. Then we get the list of all the rides
with the given source and destination. Then we convert the timestamp to required format so that we can compare it with 
the current time and date. Then we use the python module to get the current time and date.
Then the timestamp of each ride with current time and date, and if the timestamp of the ride is greater than current time,
then append the rideid, username and timestamp to the result and return the result.
If there are no upcoming rides then return 204 no content, else return the list of all upcoming rides with 200 as status code.
"""
@app.route('/api/v1/rides',methods=['GET'])
def upcoming_ride():
    source=int(request.args.get("source"))
    dest=int(request.args.get("destination"))
    
    r1=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8000/api/v1/db/read',
    json={
        "table":"Area",
        "columns":["areaname"],
        "where":["areaid='"+str(source)+"'"]
    })
    count1=r1.json().get("count")

    if count1>0:
        r2=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8000/api/v1/db/read',
        json={
            "table":"Area",
            "columns":["areaname"],
            "where":["areaid='"+str(dest)+"'"]
        })

        
        count2=r2.json().get("count")

        if count2>0:

            r=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8000/api/v1/db/read',
            json={
                "table":"Ride",
                "columns":["rideid","username","timestamp"],
                "where":["source='"+str(source)+"'","dest='"+str(dest)+"'"]
            })
           
            count=r.json().get("count")
            if count>0:
                ans=[]
            
                res_rideid=r.json().get("rideid")
                res_username=r.json().get("username")
                res_timestamp=r.json().get("timestamp")

                for i in range(count):
                    time_comp=res_timestamp[i]
                    day= time_comp[:2]
                    month= time_comp[3:5]
                    year= time_comp[6:10]
                    sec= time_comp[11:13]
                    min= time_comp[14:16]
                    hour=time_comp[17:19]

                    cur_time= (datetime.now().strftime("%d-%m-%Y:%S-%M-%H"))
                    cur_day= cur_time[:2]
                    cur_mon= cur_time[3:5]
                    cur_year= cur_time[6:10]
                    cur_sec= cur_time[11:13]
                    cur_min= cur_time[14:16]
                    cur_hour= cur_time[17:19]

                    cur= datetime(int(cur_year),int(cur_mon),int(cur_day),int(cur_hour),int(cur_min),int(cur_sec))
                    comp= datetime(int(year),int(month),int(day),int(hour),int(min),int(sec))

                    if comp>cur:

                        res={}
                        res["rideId"]=res_rideid[i]
                        res["username"]=res_username[i]
                        res["timestamp"]=res_timestamp[i]
                        ans.append(res)

                if len(ans)>0:
                    return json.dumps(ans),200

                else:
                    return json.dumps({}),400

            elif count==0:
                #no upcoming rides
                return json.dumps({}),204

            else:
                #invalid source or destination
                return json.dumps({}),400
        
        else:
            #invalid destination
            return json.dumps({}),400
    
    else:
        #invalid source
        return json.dumps({}),400


"""This API is used to get the ride details of a ride with a rideid give in url.
First we check if the rideid exists, and if the rideid exists, we get the details of 
who created the ride, source, destination, timestamp. If the rideid doesn't exist then 
return 204 as status code since there was no content to send.
Then we get the details i.e users who have joined this Ride by reading the RideUser table 
which has rideid equal to the given rideid. Then we create a string of all users 
who have joined this ride and store it in res and return it with status code of 200 
"""
@app.route('/api/v1/rides/<rideId>',methods=['GET'])
def ride_detail(rideId):
    
    r=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8000/api/v1/db/read',
    json={
        "table":"Ride",
        "columns":["rideid","username","timestamp","source","dest"],
        "where":["rideid='"+str(rideId)+"'"]
    })

    count= r.json().get("count")
    if count>0:
        res={}
        res["rideId"]= rideId
        res["created_by"]= r.json().get("username")[0]
        res["timestamp"]= r.json().get("timestamp")[0]
        res["source"]= r.json().get("source")[0]
        res["destination"]= r.json().get("dest")[0]

        res["users"]=[]

        r1=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8000/api/v1/db/read',
        json={
            "table":"RideUser",
            "columns":["username"],
            "where":["rideid='"+str(rideId)+"'"]
        })

        count1=r1.json().get("count")
        if count1>0:
            u_join= r1.json().get("username")
            joined=""
            for i in u_join:
                joined+= i+", "
            joined=joined[:-2]
            res["users"]= joined
       
        return json.dumps(res),200


    else:
        return json.dumps({}),204



"""This API is used to join a ride with rideid given in the url.
First we check if there is a ride with the given rideid.
if there is no such rideid then return 400, else check if the given username is valid (user is present)
If the username is not valid then return 400, else call the write database API.
flag set to 0 indicates the query is related to insert. 
The final query will be INSERT INTO RideUser(username,rideid) VALUES(uname,rideid);
if this query is executed successfully then 200 is returned else 400 is returned"""
@app.route('/api/v1/rides/<rideId>',methods=['POST'])
def join_ride(rideId):
    req= request.get_json()
    uname= req.get("username")
    
    r=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8000/api/v1/db/read',
    json={
        "table":"Ride",
        "columns":["rideid"],
        "where":["rideid='"+str(rideId)+"'"]
    })

    count=r.json().get("count")
    if count>0:
        r1=requests.get('http://ec2-3-222-255-47.compute-1.amazonaws.com:8080/api/v1/users').json()
        if(uname in r1):
            r2=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8000/api/v1/db/write',
            json={
                "table":"RideUser",
                "columns":["username","rideid"],
                "flag":0,
                "values":[uname,rideId]
                
            })

            count2=r2.json().get("count")
            if count2>0:
                res={}
                return jsonify({}),200

            else:
                #unsuccessful
                return json.dumps({}),400

        else:
            #user doesn't exist
            return json.dumps({}),400

    else:
        #invalid rideid, rideid doesn't exist
        return json.dumps({}),400


"""This API is used to delete a Ride given a rideid in the url.
First we read from the database if the given rideid is present or not.
If its not present then we return 405 status code.
If the rideid is present in our Ride table, then we send a request to the write database,
with flag set to 1 which indicates the query is related to delete.
The final query would be 'DELETE FROM Ride WHERE rideid= rideid' .
If this query was executed successfully, then return 200 else return 400 as it was unsuccessful"""
@app.route('/api/v1/rides/<rideId>',methods=['DELETE'])
def delete_ride(rideId):

    r=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8000/api/v1/db/read',
    json={
        "table":"Ride",
        "columns":["rideid"],
        "where":["rideid='"+str(rideId)+"'"]
    })

    count=r.json().get("count")
    if count>0:
        r1=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8000/api/v1/db/write',
        json={
            "table":"Ride",
            "flag":1,
            "condition":["rideid="+str(rideId)]
        })

        count1=r1.json().get("count")
        if count1==1:
       
            return json.dumps({}),200
        else:
          
            return json.dumps({}),400
    else:
     
        return json.dumps({}),405


"""This API is used to clear the database and return status code 200 on success.
This API first sends a write request to write database, with flag set to 1. 
Here flag=1 indicates that the query is related to delete. If the condition
is an empty list, then the query would be "DELETE FROM User" for table="User"
Similarly we clear the database by clearing all the tables and returning appropriate status code.
"""
@app.route('/api/v1/db/clear',methods=['POST'])
def cleardb():
        r1=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8000/api/v1/db/write',
        json={
            "table":"Ride",
            "flag":1,
            "condition":[]
        })

        count2= r1.json().get("count")
        if count2>0:
            r2=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8000/api/v1/db/write',
            json={
                "table":"RideUser",
                "flag":1,
                "condition":[]
            })

            count3= r2.json().get("count")
            if count3>0:
                return json.dumps({}),200
            else:
                return json.dumps({}),400
        else:
            return json.dumps({}),400
 
 
if __name__ == '__main__':	
	app.debug=True
	app.run(host="0.0.0.0")
