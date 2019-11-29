import csv
import psycopg2
import pandas as pd 

print("Connecting to all the servers")

samphat = psycopg2.connect(host="10.100.53.25",   port=2345, database="naveen_cluster", user="postgres", password="it701");
kushal = psycopg2.connect(host="10.100.54.83",port=2345, database="naveen_cluster", user="postgres", password="it701");
niranjan= psycopg2.connect(host="10.100.53.121",port=5432, database="naveen_cluster1", user="postgres", password="it701");


print("All servers are connected")

#Established a curser
cur_samphat = samphat.cursor()
cur_kushal = kushal.cursor()
cur_niranjan = niranjan.cursor()



#drop tables
cur_samphat.execute("DROP TABLE Echo1cluster;");
cur_kushal.execute("DROP TABLE Echo2cluster;");
cur_niranjan.execute("DROP TABLE Echo3cluster;");




#create tables  Echocluster1, Echocluster2, Echocluster3 in samphat, kushal, niranjan pc
cur_samphat.execute("CREATE TABLE Echo1cluster (survival real,Still_alive integer,Age_at_heart_attack real ,Pericardial_effusion integer ,Fractional_shortening real ,epss real ,lvdd  real,Wall_motion_score real ,Wall_motion_index real ,mult real ,Fulll_name varchar(50) ,grp integer ,Alive_at_1 integer);");
cur_kushal.execute("CREATE TABLE Echo2cluster (survival real,Still_alive integer,Age_at_heart_attack real ,Pericardial_effusion integer ,Fractional_shortening real ,epss real ,lvdd real,Wall_motion_score real ,Wall_motion_index real ,mult real ,Fulll_name varchar(50) ,grp integer ,Alive_at_1 integer);");
cur_niranjan.execute("CREATE TABLE Echo3cluster (survival real,Still_alive integer,Age_at_heart_attack real ,Pericardial_effusion integer ,Fractional_shortening real ,epss real ,lvdd real ,Wall_motion_score real ,Wall_motion_index real ,mult real ,Fulll_name varchar(50) ,grp integer ,Alive_at_1 integer);");




# we have to keep a record like where which cluster on which site and what is minimum and maximum range  of that particular attribute (i.e survey in this )
ip = ["10.100.55.79","10.100.54.83","10.100.54.95"] 
min = [] 
max = [] 



# finding minimum value of attribute survey in each clusters 
#write to servers
minm=500
maxm=-1
with open('echo1.txt','r') as f:   
    for row in f:
        list=row.split(',')
        #print(list[0])
        temp=float(list[0])
        #print(temp)
        if(minm>temp):
            minm=temp
        if(maxm<temp):
            maxm=temp

min.append(minm)
max.append(maxm)
print("maximum is {}".format(maxm))
print("minimum is {}".format(minm))
print(min)
print(max)


minm=500
maxm=-1
with open('echo2.txt','r') as f:   
    for row in f:
        list=row.split(',')
        #print(list[0])
        temp=float(list[0])
        #print(temp)
        if(minm>temp):
            minm=temp
        if(maxm<temp):
            maxm=temp

min.append(minm)
max.append(maxm)
print("maximum is {}".format(maxm))
print("minimum is {}".format(minm))
print(min)
print(max)


minm=500
maxm=-1
with open('echo3.txt','r') as f:   
    for row in f:
        list=row.split(',')
        #print(list[0])
        temp=float(list[0])
        #print(temp)
        if(minm>temp):
            minm=temp
        if(maxm<temp):
            maxm=temp

print("maximum is {}".format(maxm))
print("minimum is {}".format(minm))
min.append(minm)
max.append(maxm)
print(min)
print(max)




dict = {'Ip': ip, 'Minimum': min, 'Maximum': max} 
	
df = pd.DataFrame(dict) 

#creating a csv file to keep the track
df.to_csv('index.csv', header=False, index=False) 


# inserting cluster 1 to samphat pc 
with open('echo1.txt','r') as f:
    for row in f:
        
        list=row.split(',')
        #print(list)
        a=list[0]
        b=list[1] 
        c=list[2]
        d=list[3]
        e=list[4]
        f=list[5]
        g=list[6]
        h=list[7]
        i=list[8]
        j=list[9]
        k=list[10]
        l=list[11]
        m=list[12]

        
        cur_samphat.execute("INSERT INTO Echo1cluster VALUES (%s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (a,b,c,d,e,f,g,h,i,j,k,l,m))

#inserting cluster2 to kushal pc
with open('echo2.txt','r') as f:
    for row in f:
        
        list=row.split(',')
        #print(list)
        a=list[0]
        b=list[1] 
        c=list[2]
        d=list[3]
        e=list[4]
        f=list[5]
        g=list[6]
        h=list[7]
        i=list[8]
        j=list[9]
        k=list[10]
        l=list[11]
        m=list[12]

        
        cur_kushal.execute("INSERT INTO Echo2cluster VALUES (%s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (a,b,c,d,e,f,g,h,i,j,k,l,m))


#inserting cluster3 to niranjan pc

with open('echo3.txt','r') as f:
    for row in f:
        
        list=row.split(',')
       # print(list)
        a=list[0]
        b=list[1] 
        c=list[2]
        d=list[3]
        e=list[4]
        f=list[5]
        g=list[6]
        h=list[7]
        i=list[8]
        j=list[9]
        k=list[10]
        l=list[11]
        m=list[12]

        
        cur_niranjan.execute("INSERT INTO Echo3cluster VALUES (%s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (a,b,c,d,e,f,g,h,i,j,k,l,m))




# closing the connection 
cur_samphat.close()
cur_kushal.close()
cur_niranjan.close()

#commit the transaction 
samphat.commit()
kushal.commit()
niranjan.commit()


