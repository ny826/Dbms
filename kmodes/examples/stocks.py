#!/usr/bin/env python

import numpy as np
from kmodes.kprototypes import KPrototypes

# stocks with their market caps, sectors and countries
syms = np.genfromtxt('iris.csv', dtype=str, delimiter=',')[:, 1]
X = np.genfromtxt('iris.csv', dtype=object, delimiter=',')[:, 2:]
X[:, 0] = X[:, 0].astype(float)

kproto = KPrototypes(n_clusters=4, init='Cao', verbose=2)
clusters = kproto.fit_predict(X, categorical=[1, 2])

# Print cluster centroids of the trained model.
print(kproto.cluster_centroids_)
# Print training statistics
print(kproto.cost_)
print(kproto.n_iter_)

for s, c in zip(syms, clusters):
    print("Symbol: {}, cluster:{}".format(s, c))
