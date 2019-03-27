from operator import add
import time
import random
from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF, IDF
# sc = SparkContext(appName="LeibnizPI")
sc = SparkContext(appName="Text")

print("loading files...")
st = time.time()

files = sc.textFile("pq*.csv") \
.map(lambda line: line.split(",")[1]) \
.cache()

candidate = files.take(random.randint(0,1000))
# print(sample)

print("training, total time:", time.time()-st)

hashingTF = HashingTF()
tf = hashingTF.transform(files)
tf.cache()

idf = IDF().fit(tf)
tfidf = idf.transform(tf)

print("training, total time:", time.time()-st)

# candidate = clean(d2['body'][4])
# print(candidate)
candidateTf = hashingTF.transform(candidate)
candidateTfIdf = idf.transform(candidateTf)
similarities = tfidf.map(lambda v: v.dot(candidateTfIdf) / (v.norm(2) * candidateTfIdf.norm(2)))
for i in enumerate(similarities.collect()):
    print(i)

%time topFive = sorted(enumerate(similarities.collect()), key= lambda kv: kv[1])[0:5]
# topFive = sorted(enumerate(similarities.collect()), key=lambda (k, v): -v)[0:5]
for idx, val in topFive:
    print("doc '%s' has score %.4f" % (d2['body'][idx], val))

# def train():
    
# def evaluate():


# def Msum(n):
#     megaN = 1000000*n
#     s = 0.0
#     for k in range(1000000):
#         d0 = 4*(k+1+megaN)+1
#         d1 = d0-2
#         s += ((1.0/d0)-(1.0/d1))
#     return s

# def LeibnizPi(Nterms):
#     sc = SparkContext(appName="LeibnizPI")
#     piOver4Minus1 = sc.parallelize(range(0,Nterms+1), 20).map(Msum).reduce(add)
#     sc.stop()
#     return 4*(1+piOver4Minus1)

# print(LeibnizPi(999))
# sc.stop()
