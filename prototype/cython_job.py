from operator import add
from pyspark import SparkContext
import cython
# sc = SparkContext(appName="LeibnizPI")


import sys, os, shutil, cython

def spark_cython(module, method):
    def wrapped(*args, **kwargs):
#         print('Entered function with: %s' % args)
        global cython_function_
        try:
            return cython_function_(*args, **kwargs)
        except:
            import pyximport
            pyximport.install()
#             print('Cython compilation complete')
            cython_function_ = getattr(__import__(module), method)
#             print('Defined function: %s' % cython_function_)
        return cython_function_(*args, **kwargs)
    return wrapped
mapper = spark_cython('mfun','Msum_c')

# def Msum(n):
#     megaN = 1000000*n
#     s = 0.0
#     for k in range(1000000):
#         d0 = 4*(k+1+megaN)+1
#         d1 = d0-2
#         s += ((1.0/d0)-(1.0/d1))
#     return s

def LeibnizPi(Nterms):
    sc = SparkContext(appName="LeibnizPICython")
    sc.addPyFile("mfun.pyx")
    piOver4Minus1 = sc.parallelize(range(0,Nterms+1), 20).map(mapper).reduce(add)
    sc.stop()
    return 4*(1+piOver4Minus1)

print(LeibnizPi(999))
