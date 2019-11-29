import os

#jaccard similarity between two strings 
def jaccard_similarity(query, document):
    intersection = set(query).intersection(set(document))
    union = set(query).union(set(document))
    return len(intersection)/len(union)



cluster1=""
cluster2=""
cluster3=""
cluster4=""


# cluster1 stores the cluster1.txt file 
with open('echo1.txt','r') as f:
    for row in f:
        cluster1+=row
        cluster1+="\n"


# cluster2 stores the cluster2.txt file 
with open('echo2.txt','r') as f:
    for row in f:
      

        cluster2+=row
        cluster2+="\n"


# cluster3 stores the cluster3.txt file 
with open('echo3.txt','r') as f:
    for row in f:        
        cluster3+=row
        cluster3+="\n"


# cluster4 stores the cluster4.txt file 
with open('echo4.txt','r') as f:
    for row in f:       
        cluster4+=row
        cluster4+="\n"



#finding similarity between each pairs and there are 4C2 pairs i.e 6 pairs 
cluster_12=jaccard_similarity(cluster1,cluster2)
print(cluster_12)
cluster_13=jaccard_similarity(cluster1,cluster3)
print(cluster_13)
cluster_14=jaccard_similarity(cluster1,cluster4)
print(cluster_14)
cluster_23=jaccard_similarity(cluster2,cluster3)
print(cluster_23)
cluster_24=jaccard_similarity(cluster2,cluster4)
print(cluster_24)
cluster_34=jaccard_similarity(cluster3,cluster4)
print(cluster_34)



#finding maximum simarity between pairs and merger the clusters based on them 
max=0
if(cluster_12>max):
    max=cluster_12

if(cluster_13>max):
    max=cluster_13

if(cluster_14>max):
    max=cluster_14

if(cluster_23>max):
    max=cluster_23

if(cluster_24>max):
    max=cluster_24

if(cluster_34>max):
    max=cluster_34

print(" Maximum similarity is {}".format(max))



if(max==cluster_12):
    print("merging cluter 1 and cluster 2 and deleting cluter2.txt")
    with open("echo2.txt") as f:
        with open("echo1.txt", "w") as f1:
            for line in f:            
                f1.write(line) 

    os.remove("echo2.txt")



elif(max==cluster_13):

    print("merging cluter 1 and cluster 3 and deleting cluster3.txt")

    with open("echo3.txt") as f:
        with open("echo1.txt", "w") as f1:
            for line in f:            
                f1.write(line) 

    os.remove("echo3.txt")



elif(max==cluster_14):

    print("merging cluter 1 and cluster 4 and deleting cluster4.txt")

    with open("echo4.txt") as f:
        with open("echo1.txt", "w") as f1:
            for line in f:            
                f1.write(line) 
        
    os.remove("echo4.txt")


elif(max==cluster_23):


    print("merging cluter 2 and cluster 3 and deleting cluter3.txt")

    with open("echo3.txt") as f:
        with open("echo2.txt", "w") as f1:
            for line in f:            
                f1.write(line) 

    os.remove("echo3.txt")



elif(max==cluster_24):


    print("merging cluster 2 and cluster 4 and deleting cluter4.txt")

    with open("echo4.txt") as f:
        with open("echo2.txt", "w") as f1:
            for line in f:            
                f1.write(line) 

    os.remove("echo4.txt")



elif(max==cluster_34):


    print("merging cluster 3 and cluster 4 and deleting cluster4.txt")

    with open("echo4.txt") as f:
        with open("echo3.txt", "w") as f1:
            for line in f:            
                f1.write(line) 

    os.remove("echo4.txt")



