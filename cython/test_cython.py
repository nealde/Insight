from cy_utils2 import cos
import numpy as np

#a = np.zeros((200,2),dtype=np.int32)
#i = np.arange(0,40).astype(np.int32)
#v = np.arange(0,40).astype(np.float64)
#i2 = np.arange(5,70).astype(np.int32)
#v2 = np.arange(5,70).astype(np.float64)

a = np.arange(0,40).astype(np.int32)
b = np.arange(5,70).astype(np.int32)
c = np.arange(0,40).astype(np.float64)
d = np.arange(5,70).astype(np.float64)

def cos_np(inds1,vals1,inds2,vals2):
    i1 = np.where(np.isin(inds1,inds2))
    i2 = np.where(np.isin(inds2,inds1))
    product = np.sum(vals1[i1]*vals2[i2])
    return product/np.linalg.norm(vals1)/np.linalg.norm(vals2)

print(cos_np(a,c,b,d))

dd = np.zeros((1000,2),dtype=np.int32)
for i in range(2,400):
    for j in range(2,150):
        a = np.arange(0,i).astype(np.int32)
        b = np.arange(0,j).astype(np.int32)
        c = np.arange(0,i).astype(np.float64)
        d = np.arange(0,j).astype(np.float64)
        f = cos(a,c,b,d,dd)

#print(np.linalg.norm(d))
#print(norm2(d))

#print(i, len(i), i2, len(i2))
#print(cos(a,c,b,d,np.zeros((200,2),dtype=np.int32)))
#print(cos(i,v,i2,v2,a))

