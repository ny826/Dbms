import csv
import psycopg2

print("Connecting to all the servers")

samphat = psycopg2.connect(host="10.100.55.79",   port=2345, database="naveen", user="postgres", password="it701");
kushal = psycopg2.connect(host="10.100.54.83",port=2345, database="naveen", user="postgres", password="it701");
niranjan= psycopg2.connect(host="10.100.54.95",port=5432, database="naveen", user="postgres", password="it701");


print("All servers are connected")

cur_samphat = samphat.cursor()
cur_kushal = kushal.cursor()
cur_niranjan = niranjan.cursor()


'''
#drop tables
cur_samphat.execute("DROP TABLE Echo1;");
cur_kushal.execute("DROP TABLE Echo2;");
cur_niranjan.execute("DROP TABLE Echo3;");
'''



#create tables  Echo1 Echo2 Echo3 
cur_samphat.execute("CREATE TABLE Echo1 (survival real,Still_alive integer,Age_at_heart_attack real ,Pericardial_effusion integer ,Fractional_shortening real ,epss real ,lvdd  real,Wall_motion_score real ,Wall_motion_index real ,mult real ,Fulll_name varchar(50) ,grp integer ,Alive_at_1 integer);");
cur_kushal.execute("CREATE TABLE Echo2 (survival real,Still_alive integer,Age_at_heart_attack real ,Pericardial_effusion integer ,Fractional_shortening real ,epss real ,lvdd real,Wall_motion_score real ,Wall_motion_index real ,mult real ,Fulll_name varchar(50) ,grp integer ,Alive_at_1 integer);");
cur_niranjan.execute("CREATE TABLE Echo3 (survival real,Still_alive integer,Age_at_heart_attack real ,Pericardial_effusion integer ,Fractional_shortening real ,epss real ,lvdd real ,Wall_motion_score real ,Wall_motion_index real ,mult real ,Fulll_name varchar(50) ,grp integer ,Alive_at_1 integer);");


# finding no of tuples in database 
length=1
with open('echo.csv','r') as f:
    reader = csv.reader(f) 
   
    for row in reader:
        length=length+1





with open('echo.csv','r') as f:
    reader = csv.reader(f) 
    count=1
    for row in reader:
        a=row[0]
        b=row[1]                
        c= row[2]
        d=row[3]
        e=row[4]
        f=row[5]
        g=row[6]
        h=row[7]
        i=row[8]
        j=row[9]
        k=row[10]
        l=row[11]
        m=row[12]

        if count < length/3:
                       
                cur_samphat.execute("INSERT INTO Echo1 VALUES (%s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (a,b,c,d,e,f,g,h,i,j,k,l,m))

        elif count>=length/3 and count < (2*length)/3:
                cur_kushal.execute("INSERT INTO Echo2 VALUES (%s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (a,b,c,d,e,f,g,h,i,j,k,l,m))
                
        elif count >= (2*length)/3:
                cur_niranjan.execute("INSERT INTO Echo3 VALUES (%s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (a,b,c,d,e,f,g,h,i,j,k,l,m))





        
        count = count + 1    

cur_samphat.close()
cur_kushal.close()
cur_niranjan.close()

samphat.commit()
kushal.commit()
niranjan.commit()


