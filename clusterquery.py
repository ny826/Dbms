import time
import csv
import psycopg2

print("Connecting to all the servers")

samphat = psycopg2.connect(host="10.100.53.25",   port=2345, database="naveen_cluster", user="postgres", password="it701");
kushal = psycopg2.connect(host="10.100.54.83",port=2345, database="naveen_cluster", user="postgres", password="it701");
niranjan= psycopg2.connect(host="10.100.53.121",port=5432, database="naveen_cluster1", user="postgres", password="it701")#;


range =31.0
print("All servers are connected")

cur_samphat = samphat.cursor()
cur_kushal = kushal.cursor()
cur_niranjan = niranjan.cursor() 

start_time=time.time();


#Query1
print("Query1 Using Clusters SELECT * FROM Echo WHERE survival >=31")
with open('index.csv','r') as f:
    reader = csv.reader(f) 
   
    for row in reader:
        ip =row[0]
        min=float(row[1])
        max=float(row[2])
       
        if range>=  min and range<= max :
           
            if(ip=="10.100.53.25"): 
                cur_samphat.execute("SELECT * FROM Echo1cluster WHERE survival >=31")
                rows1 = cur_samphat.fetchall()
                #print("# Records in samphat  for query1 is :{}".format(len(rows1)))
            if(ip=="10.100.54.83"):
                cur_kushal.execute("SELECT * FROM Echo2cluster WHERE survival >=31")
                rows1 = cur_kushal.fetchall()
                #print("# Records in kushal  for query1 is : {}".format(len(rows1)))           
            if(ip=="10.100.53.121"):
                cur_niranjan.execute("SELECT * FROM Echo3cluster WHERE survival >=31")
                rows1 = cur_niranjan.fetchall()
                #print("# Records in niranjan  for query1 is :{}".format(len(rows1)))   


end_time=time.time();


print("Time to Execute Query is  ")
print((end_time-start_time)*1000);





cur_samphat.close()
cur_kushal.close()
cur_niranjan.close()


samphat.commit()
kushal.commit()
niranjan.commit()


