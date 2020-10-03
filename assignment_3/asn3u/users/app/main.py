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
        conn=sqlite3.connect("user.db")
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
    
    elif flag==2:              #update
        columns= req.get("columns")
        sett= req.get("sett")
        print("SETTTT, COLUMNSSSS",sett,columns)
        final_columns=''
        for i in columns:
            final_columns+="'"+ i+"', "
        final_columns=final_columns[:-2]

        query='UPDATE '+table+' SET '
        print(query)
        for i in range(len(sett)):
            query += columns[i]+" = "+str(sett[i])+", "
        query=query[:-2]
        query+= ";" 

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
        conn=sqlite3.connect("user.db")
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
            print(e)
            res["count"]=0
            res["status"]=400
            conn.close()
            return json.dumps(res)

    except Exception as err:
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


"""This API is used to count the http requests.
We have a table called count which has column called total
This API returns the count of HTTP requests. It sends the request to read database.
If the returned list length is 0 then it means that there were 0 requests made and hence return [0]"""
@app.route('/api/v1/_count',methods=['GET'])
def _count():
    r=requests.post('http://ec2-52-20-14-30.compute-1.amazonaws.com:80/api/v1/db/read',
    json={
        'table':'count',
        'columns':['total'],
        'where':[]
    })

    total= r.json().get('total')
    if len(total)==0:
        return json.dumps([0]),200
    return json.dumps([total[0]]),200


"""This API is used to reset the count of HTTP requests.
This API calls the write API which is in the orchestrator instance.
flag=2 indicates that this is a update query.
sett=[0] indicates that the column named total to be set to 0
count>0 then the reset was successful and hence return 200, else return 400"""
@app.route('/api/v1/_count',methods=['DELETE'])
def deletecount():
    r=requests.post('http://ec2-52-20-14-30.compute-1.amazonaws.com:80/api/v1/db/write',
    json={
        'table':'count',
        'flag':2,
        'columns':['total'],
        'sett':[0]
    })

    count= r.json().get('count')
    if count>0:
        return json.dumps({}),200
    return json.dumps({}),400


"""This function is used to increment the count of and write it to database.
At first, we read the number of HTTP requests made from the database by calling the read API,
and then increment the value and write it to the database by calling the write API.
If it was incremented successfully, then return 200, else return 400 """
def count_write():

    r1=requests.post('http://ec2-52-20-14-30.compute-1.amazonaws.com:80/api/v1/db/read',
    json={
        'table':'count',
        'columns':['total'],
        'where':[]
    })

    count1= r1.json().get('count')
    if count1>0:
        total= r1.json().get('total')
        print("ITS SETTTTT",total)
        r=requests.post('http://ec2-52-20-14-30.compute-1.amazonaws.com:80/api/v1/db/write',
        json={
            'table':'count',
            'flag':2,
            'columns':['total'],
            'sett':[total[0]+1]
        })

        return json.dumps({}),200
    else:
        return json.dumps({}),400


"""Since we need to count the number of HTTP request, we use these APIs which are called when 
a wrong method is used. For example here /api/v1/db/clear, the correct method is POST method,
but if other method was used with the same url, then this API gets called which calls the count_write function.
That function as explained above increments the count and returns the appropriate status code.
At last we are returning 405, because this is the correct status code for wrong method used (method not allowed error)"""
@app.route('/api/v1/db/clear',methods=['GET','PUT','PATCH','DELETE','COPY','HEAD','OPTIONS','LINK','UNLINK','PURGE','LOCK','UNLOCK','PROPFIND','VIEW'])
def m1():
    count_write()
    return jsonify({}),405


"""This API is used to clear the database and return status code 200 on success.
This API first sends a write request to write database, with flag set to 1. 
Here flag=1 indicates that the query is related to delete. If the condition
is an empty list, then the query would be "DELETE FROM User" for table="User"
Similarly we clear the database by clearing all the tables and returning appropriate status code.
"""
@app.route('/api/v1/db/clear',methods=['POST'])
def cleardb():

    count_write()
    r=requests.post('http://ec2-52-20-14-30.compute-1.amazonaws.com:80/api/v1/db/write',
    json={
          "table":"User",
          "flag":1,
          "condition":[]
    })

    count=r.json().get("count")
    if count>0:
        return json.dumps({}),200

    else:
        return json.dumps({}),400


"""As explained before this API is just to increment the count of HTTP request"""
@app.route('/api/v1/users',methods=['POST','PATCH','DELETE','COPY','HEAD','OPTIONS','LINK','UNLINK','PURGE','LOCK','UNLOCK','PROPFIND','VIEW'])
def m2():
    count_write()
    return jsonify({}),405


"""This API is used to add a new user. First we check if the password given is in sha format.
if its not in sha format return 400 as status code, else we check if the username given is 
already in the User table. Since the username should be unique if the username given is already 
in our database then return 400 as status code, else send a request to the write database API
with flag set to 0 indicating that the query will be related to insert. The final query will be
INSERT INTO USER(username, password) VALUES(uname,password); On successful execution of this
query, we send the status code of 201 which is meant for successfully created, else we return 400 """
@app.route('/api/v1/users',methods=['PUT'])
def Add_user():
    
    count_write()
    req= request.get_json()
    uname= req.get("username")
    print(uname)
    password= req.get("password")

    if(uname=="" or password==""):
        return json.dumps({}),400

    if len(password)!=40:
     
        return json.dumps({}),400    
    try:
        sha=int(password,16)
    except ValueError:
        #return response message as invalid password
        return json.dumps({}),400



    r=requests.post('http://ec2-52-20-14-30.compute-1.amazonaws.com:80/api/v1/db/read',
    json={
        'table':'User',
        'columns':['username'],
        'where':["username='"+uname+"'"]

    })

    count=r.json().get("count")
    if count>0:
        status_code=r.json().get("status")
        #return username already exists
        return json.dumps({}),400
        
    else:
        r=requests.post('http://ec2-52-20-14-30.compute-1.amazonaws.com:80/api/v1/db/write',
        json={
            'table':'User',
            'flag':0,
            'columns':['username','password'],
            'values':[uname,password]

        })

        status_code=r.json().get("status")
        count=r.json().get("count")
        res={}
        print(count)
        if count==1:
            res["count"]=count
            
            return json.dumps({}),201
        
        else:
            return json.dumps({}),400


"""As explained before this API is just to increment the count of HTTP request"""
@app.route('/api/v1/users/<username>',methods=['GET','PUT','PATCH','POST','COPY','HEAD','OPTIONS','LINK','UNLINK','PURGE','LOCK','UNLOCK','PROPFIND','VIEW'])
def m3():
    count_write()
    return jsonify({}),405


"""This API is used to delete a user with username given in the url.
First we check if the username given is a valid i.e if the user exists by calling the read database API.
If the user doesn't exist return status code of 400, else send a request to write database API with
flag set to 1 which indicates the query will be related to delete. The final query will be
DELETE FROM USER where username= username; on successfully executing this query we have to delete all
the rides associated with this user. So we have written another API in rides which will delete all the rides
created by this user. On success return 200 as status code, else return 400"""
@app.route('/api/v1/users/<username>',methods=['DELETE'])
def REMOVE_user(username):

    count_write()

    r=requests.post('http://ec2-52-20-14-30.compute-1.amazonaws.com:80/api/v1/db/read',
    json={
        'table':'User',
        'columns':['username'],
        'where':["username='"+username+"'"]
    })
    
    count=r.json().get("count")
    if count>0:
        r1=requests.post('http://ec2-52-20-14-30.compute-1.amazonaws.com:80/api/v1/db/write',
        json={
            'table':'User',
            'flag':1,
            'condition':["username = '"+username+"'"]
        })
        status_code=r1.json().get("status")
        count1=r1.json().get("count")

        if count1>0:
            requests.post('http://asn-3-1766468858.us-east-1.elb.amazonaws.com:80/api/v1/deleteride',
            json={
                'username':str(username)
            })
            
            return json.dumps({}),200
        else:
            return json.dumps({}),400

    else:
        #return username doesn't exist
        return json.dumps({}),400


"""This API is used to get the list of all users. This API is called from the rides instance
while adding a new ride to check of the user is a valid user. This API sends a request to 
the read database API. The query would be SELECT username FROM User; If this query is successful
and if there is atleast one user then we return the list of users along with 200 as status code,
else we return 204 as there were 0 users. (204 is used for no content available)"""
@app.route('/api/v1/users',methods=['GET'])
def userss():

    count_write()
    r= requests.post('http://ec2-52-20-14-30.compute-1.amazonaws.com:80/api/v1/db/read',
    json={
        "table":"User",
        "columns":["username"],
        "where":[]
    })
    count= r.json().get("count")

    if count>0:
        users= r.json().get("username")
        return json.dumps(users),200
    else:
        return json.dumps({}),204


if __name__ == '__main__':	
	app.debug=True
	app.run(host="0.0.0.0")
