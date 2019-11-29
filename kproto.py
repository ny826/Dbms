import numpy as np
from kmodes.kprototypes import KPrototypes

#Data points with their publisher name,category score, category name, place name
syms = np.genfromtxt('DATASETS/census.csv', dtype=str, delimiter=',')[1:]
print(syms)

X = np.genfromtxt('DATASETS/census.csv', dtype=object, delimiter=',')[1:]
X[:, 0] = X[:, 0].astype(float)
print(X[:,0])

kproto = KPrototypes(n_clusters=4, init='Huang', verbose=1)
print("printing kproto {}".format(kproto))
clusters = kproto.fit_predict(X, categorical=[1,3,5,6,7,8,9,13,14])
print("printing cluster 0 {}".format(clusters[0]))
print(clusters[1])
print(clusters[2])
print(clusters[3])

# Print cluster centroids of the trained model.
print(kproto.cluster_centroids_)
# Print training statistics
clst0 , clst1 , clst2 , clst3 =[] , [] , [] , []
for i in range(len(clusters)):
	if clusters[i] == 0:
		clst0.append(syms[i])
	elif clusters[i] == 1:
		clst1.append(syms[i])
	elif clusters[i] == 2:
		clst2.append(syms[i])
	else:
		clst3.append(syms[i])	

with open('cs1.txt','w') as file:
	for g in clst0:
		lst = ''
		for l in g:
			lst += l+','
		file.write(lst[:-1]+'\n')	

with open('cs2.txt','w') as file:
	for g in clst1:
		lst = ''
		for l in g:
			lst += l+','
		file.write(lst[:-1]+'\n')

with open('cs3.txt','w') as file:
	for g in clst2:
		lst = ''
		for l in g:
			lst += l+','
		file.write(lst[:-1]+'\n')
		
with open('cs4.txt','w') as file:
	for g in clst3:
		lst = ''
		for l in g:
			lst += l+','
		file.write(lst[:-1]+'\n')				



# for s, c in zip(syms, clusters):
#     lst0.append("{},{}".format(s, c))   
# # Clustered result
# result = zip(syms, clusters)
# sortedR = sorted(result, key=lambda x: x[1])
# print(sortedR)
