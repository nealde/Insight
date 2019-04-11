import cython
cimport cython

#@cython.boundscheck(False)
#@cython.wraparound(False)
cdef norm2(const double [:] a):
    cdef int k = 0
    cdef double val = 2.0
    cdef double n = 0
    for i in range(len(a)):
        k = i
        n += a[k]**val
    return n**0.5

#@cython.boundscheck(False)
#@cython.wraparound(False)
cdef common_inds(const int [:] a,const int [:] b, int [:,::] c):
    cdef int count = 0
#     print(len(a), len(b))
    cdef int n = len(b)
    cdef int m = len(a)
    cdef int i = 0
    cdef int total = 0
    cdef int k = 0
    for k in range(2*m):
        if a[i] == b[count]:
            c[total,0] = i
            c[total,1] = count
            count += 1
            total += 1
#             inds.append([i,count])
        elif a[i] > b[count]:
            i -= 1
            count += 1
        if count >= n:
            return total
        i += 1
    return total

#@cython.boundscheck(False)
#@cython.wraparound(False)
def cos(const int [:] inds1,const double [:] vals1,const int [:] inds2,const double [:] vals2, int [:,::] data_store):
    cdef double product = 0
    cdef int n = len(inds1)
    cdef int m = len(inds2)
    cdef int count = 0
    cdef int i = 0
    if n > m:
        count = common_inds(inds1, inds2, data_store)
        for i in range(count):
            product += vals1[data_store[i,0]]*vals2[data_store[i,1]]
    else:
        count = common_inds(inds2, inds1, data_store)
        for i in range(count):
            product += vals2[data_store[i,0]]*vals1[data_store[i,1]]
    product /= norm2(vals1) #*norm2(vals2))
    return product/norm2(vals2)
