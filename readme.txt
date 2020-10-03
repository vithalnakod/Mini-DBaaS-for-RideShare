There are 4 folders in the root directory. 3 folders are for 3 assignments and project folder is for the final project. 

For the assignments we have uploaded the database file directly to the aws instance, and for the project we have written a function to create the database. 

The database which was uploaded to the instance has been included in their respective folders. 

To run the assignment 1, navigate to the assignment_1 folder    ($ cd assignment_1) and then run the python file using the command below
$ python3 CC_1628_1731_1746.py
The necessary modules which need to be installed are flask, requests

To run the assignment 2, navigate to the assignment_2 folder    ($ cd assignment_2) and then run the below command
$ sudo docker-compose up --build

To run the assignment 3, navigate to the assignment_3 folder    ($ cd assignment_3) and then there will be 2 folders named asn3u which contains all files related to users instance and another asn3r 
which contains all files related to rides instance. Navigate to the two folders in two separate terminals and then run the below command
$ sudo docker-compose up --build 

To run the project, navigate to the finalproject folder ($ cd finalproject). There are 3 folders named users, rides and project. The APIs related to users are in users folder and the APIs related
to the rides are in rides folder. The read and write APIs along with crash and get all workers APIs are in project folder. This folder also contains the code whcih implements the final project.
Navigate to all the 3 folders in 3 separate terminals and then run the below command
$ sudo docker-compose up --build

All the necessary modules which needs to be installed are written in the requirements.txt file. 