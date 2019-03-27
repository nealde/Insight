# import cython
cimport cython
# from libc.math cimport sqrt

@cython.boundscheck(False)
@cython.wraparound(False)
def Msum_c(int n):
	cdef int megaN = 1000000*n
	cdef float s = 0.0
	cdef int d0 = 0
	cdef int d1 = 0
	cdef int i = 0
	for k in range(1000000):
		i = k
		d0 = 4*(i+1+megaN)+1
		d1 = d0-2
		s += ((1.0/d0)-(1.0/d1))
	return s
