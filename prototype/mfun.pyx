# import cython
cimport cython
# from libc.math cimport sqrt

@cython.boundscheck(False)
@cython.wraparound(False)
def Msum_c(int n):
    cdef double megaN = 1000000*n
    cdef double s = 0.0
    cdef double d0 = 0.0
    cdef double d1 = 0.0
    for k in range(1000000):
        d0 = 4*(k+1+megaN)+1
        d1 = d0-2
        s += ((1.0/d0)-(1.0/d1))
    return s