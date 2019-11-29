import time
import csv
import psycopg2

print("Connecting to all the servers")

samphat = psycopg2.connect(host="10.100.53.25",   port=2345, database="naveen", user="postgres", password="it701");
kushal = psycopg2.connect(host="10.100.54.83",port=2345, database="naveen", user="postgres", password="it701");
niranjan= psycopg2.connect(host="10.100.53.121",port=5432, database="naveen", user="postgres", password="it701")#;


print("All servers are connected")

cur_samphat = samphat.cursor()
cur_kushal = kushal.cursor()
cur_niranjan = niranjan.cursor() 


range=40.0
start_time = time.time()
#Execute  Query1 
print("Query 1 is SELECT * FROM Echo1 WHERE survival >=40")
if(range<20):
    cur_samphat.execute("SELECT * FROM Echo1 WHERE survival >=40");
    rows1 = cur_samphat.fetchall()
    print("# records in samphat server for query1 is : {}".format(len(rows1)))
# for row in rows1:
#    print("survival = {}".format(row[0]))
#    print("Still_alive = {}".format(row[1]))
#    print("Age_at_heart_attack = {}".format(row[2]))
#    print("Pericardial_effusion = {}".format(row[3]))
#    print("Fractional_shortening = {}".format(row[4]))
#    print("epss = {}".format(row[5]))
#    print("lvdd = {}".format(row[6]))
#    print("Wall_motion_score = {}".format(row[7]))
#    print("Wall_motion_index = {}".format(row[8]))
#    print("mult = {}".format(row[9]))
#    print("Fulll_name = {}".format(row[10]))
#    print("grp = {}".format(row[11]))
#    print("Alive_at_1 = {}".format(row[12]))

if(range>=20 and range<40):
    cur_kushal.execute("SELECT * FROM Echo2 WHERE survival >=40");
    rows2 = cur_kushal.fetchall()
    print("# Record in kushal server for query1 is : {}".format(len(rows2)))

# for row in rows2:
#    print("survival = {}".format(row[0]))
#    print("Still_alive = {}".format(row[1]))
#    print("Age_at_heart_attack = {}".format(row[2]))
#    print("Pericardial_effusion = {}".format(row[3]))
#    print("Fractional_shortening = {}".format(row[4]))
#    print("epss = {}".format(row[5]))
#    print("lvdd = {}".format(row[6]))
#    print("Wall_motion_score = {}".format(row[7]))
#    print("Wall_motion_index = {}".format(row[8]))
#    print("mult = {}".format(row[9]))
#    print("Fulll_name = {}".format(row[10]))
#    print("grp = {}".format(row[11]))
#    print("Alive_at_1 = {}".format(row[12]))

if(range>=40):
    cur_niranjan.execute("SELECT * FROM Echo3 WHERE survival >=40");
    rows3 = cur_niranjan.fetchall()
    print("# Record in niranjan server for query1  is : {}".format(len(rows3)))

# for row in rows3:
#    print("survival = {}".format(row[0]))
#    print("Still_alive = {}".format(row[1]))
#    print("Age_at_heart_attack = {}".format(row[2]))
#    print("Pericardial_effusion = {}".format(row[3]))
#    print("Fractional_shortening = {}".format(row[4]))
#    print("epss = {}".format(row[5]))
#    print("lvdd = {}".format(row[6]))
#    print("Wall_motion_score = {}".format(row[7]))
#    print("Wall_motion_index = {}".format(row[8]))
#    print("mult = {}".format(row[9]))
#    print("Fulll_name = {}".format(row[10]))
#    print("grp = {}".format(row[11]))
#    print("Alive_at_1 = {}".format(row[12]))

end_time=time.time();

print("Time to Execute Query is  ")
print((end_time-start_time)*1000);



start_time = time.time()


print("\n")
print("------------------------------------")
print("\n")



#Execute  Query2
print("Query 2 : SELECT * FROM Echo1 WHERE survival >=31")
range=31.0

if(range<20):
    cur_samphat.execute("SELECT * FROM Echo1 WHERE survival <20");
    rows1 = cur_samphat.fetchall()
    print("# records in samphat server for query2 is : {}".format(len(rows1)))

# for row in rows1:
#    print("survival = {}".format(row[0]))
#    print("Still_alive = {}".format(row[1]))
#    print("Age_at_heart_attack = {}".format(row[2]))
#    print("Pericardial_effusion = {}".format(row[3]))
#    print("Fractional_shortening = {}".format(row[4]))
#    print("epss = {}".format(row[5]))
#    print("lvdd = {}".format(row[6]))
#    print("Wall_motion_score = {}".format(row[7]))
#    print("Wall_motion_index = {}".format(row[8]))
#    print("mult = {}".format(row[9]))
#    print("Fulll_name = {}".format(row[10]))
#    print("grp = {}".format(row[11]))
#    print("Alive_at_1 = {}".format(row[12]))

if(range>=20 and range<40):
    cur_kushal.execute("SELECT * FROM Echo2 WHERE survival >=20 and survival <40");
    rows2 = cur_kushal.fetchall()
    print("# Record in kushal server for query2 is :  {}".format(len(rows2)))

# for row in rows2:
#    print("survival = {}".format(row[0]))
#    print("Still_alive = {}".format(row[1]))
#    print("Age_at_heart_attack = {}".format(row[2]))
#    print("Pericardial_effusion = {}".format(row[3]))
#    print("Fractional_shortening = {}".format(row[4]))
#    print("epss = {}".format(row[5]))
#    print("lvdd = {}".format(row[6]))
#    print("Wall_motion_score = {}".format(row[7]))
#    print("Wall_motion_index = {}".format(row[8]))
#    print("mult = {}".format(row[9]))
#    print("Fulll_name = {}".format(row[10]))
#    print("grp = {}".format(row[11]))
#    print("Alive_at_1 = {}".format(row[12]))

if(range>=31):
    cur_niranjan.execute("SELECT * FROM Echo3 WHERE survival >=40");
    rows3 = cur_niranjan.fetchall()
    print("# Record in niranjan server for query2 is : {}".format(len(rows3)))

# for row in rows3:
#    print("survival = {}".format(row[0]))
#    print("Still_alive = {}".format(row[1]))
#    print("Age_at_heart_attack = {}".format(row[2]))
#    print("Pericardial_effusion = {}".format(row[3]))
#    print("Fractional_shortening = {}".format(row[4]))
#    print("epss = {}".format(row[5]))
#    print("lvdd = {}".format(row[6]))
#    print("Wall_motion_score = {}".format(row[7]))
#    print("Wall_motion_index = {}".format(row[8]))
#    print("mult = {}".format(row[9]))
#    print("Fulll_name = {}".format(row[10]))
#    print("grp = {}".format(row[11]))
#    print("Alive_at_1 = {}".format(row[12]))

end_time=time.time();

print("Time to Execute Query is  ")
print((end_time-start_time)*1000);




cur_samphat.close()
cur_kushal.close()
cur_niranjan.close()


samphat.commit()
kushal.commit()
niranjan.commit()


