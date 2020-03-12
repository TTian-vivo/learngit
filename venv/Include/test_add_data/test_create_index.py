#!/usr/bin/python
import faiss
import numpy as np
import time

d = 10
nb = 100
nq = 10
k = 99

xq = np.random.random((nq,d)).astype("float32")

nlist = 2    ####代表分桶的个数

index1 = faiss.IndexFlatL2(d)
index = faiss.IndexIVFFlat(index1,d,nlist)

assert not index.is_trained
index.train(xb)
assert index.is_trained

index.add(xb)
index.add_with_ids(xb, ids)

ins=np.random.random((1,d)).astype("float32")

D,I = index.search(xq,k)












