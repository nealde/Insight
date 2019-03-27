from operator import add
from pyspark import SparkContext
# sc = SparkContext(appName="LeibnizPI")
from numba import jit

@jit(nopython=True)
def Msum(n):
    megaN = 1000000*n
    s = 0.0
    for k in range(1000000):
        d0 = 4*(k+1+megaN)+1
        d1 = d0-2
        s += ((1.0/d0)-(1.0/d1))
    return s

def LeibnizPi(Nterms):
    sc = SparkContext(appName="LeibnizPI_numba")
    piOver4Minus1 = sc.parallelize(range(0,Nterms+1), 20).map(Msum).reduce(add)
    sc.stop()
    return 4*(1+piOver4Minus1)

print(LeibnizPi(999))
# sc.stop()
