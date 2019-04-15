import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell'
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DataType, FloatType
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.linalg import SparseVector, VectorUDT

#    Spark Streaming
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from redis import StrictRedis
import numpy as np
import zlib
import json

# local cosine similarity in Cython
from cy_utils3 import cos


idfpath = 's3n://neal-dawson-elli-insight-data/models/idf-model3'

spark = SparkSession\
     .builder\
     .appName("streaming")\
     .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

_connection = None

def connection():
    """Singleton implementation of a Redis connection, which significantly
    speeds up bulk writes and avoids address collisions.
    """
    global _connection
    if _connection is None:
        _connection = StrictRedis.from_url('redis://10.0.0.10:6379')
    return _connection

def clean(line):
    """Given a line, clean it and return it."""
    line = line.lower().replace("\n"," ").replace("\r","").replace(',',"").replace(">","> ").replace("<", " <").replace("|"," ")
    return line

def report_to_redis(job, count=5):
    """Given a unique string 'job', connect to Redis and look at the Sorted
    Set which holds the results for that job. Take the top 5 and report them
    into the report key store."""

    # it's important that these main python methods
    # don't call the Singleton - _connection needs to be None to be
    # properly serialized.
    r = StrictRedis.from_url("redis://10.0.0.10:6379")
    for i in range(count):
        res = r.zpopmax('temp0')
        print(res)
        title = r.hget(res[0][0][:-1],res[0][0][-1:]+':t')
        r.set('success:'+str(job)+'|'+str(i), res[0][0]+'|%1.3f'%res[0][1])
    r.delete('temp0')
    return 0

def common_inds(a,b):
    inds = []
    count = 0
#     print(len(a), len(b))
    n = len(b)
#     print(n)
    m = len(a)
    total = 0
    i = 0
#     while i < m:
    for k in range(2*m):
#         print('i:', i, 'count:', count, a[i], b[count])
        total += 1
#         print(a[i], b[count])
        if a[i] == b[count]:
            inds.append([i,count])
            count += 1
        elif a[i] > b[count]:
            i -= 1
            count += 1
        if count >= n:
            break
        i += 1
    return inds

def cos_np(inds1,vals1,vals2):
#     i1 = np.where(np.isin(inds1,inds2))
#     i2 = np.where(np.isin(inds2,inds1))
    inds1 = np.array(inds1, dtype=np.int32)
    i1 = inds1[:,0]
    i2 = inds1[:,1]
    product = np.sum(vals1[i1]*vals2[i2])
    return product/np.linalg.norm(vals1)/np.linalg.norm(vals2)



def get_features(key, compare, limit=True):
    """Given the key and the target SparseVector to match, connect to Redis,
    retrieve the compressed NumPy vectors of indices and values, and then
    pass those to the cython implementation of the cosine similarity.
    The cython implementation is ~20x faster than creating a SparseVector and
    calculating the cosine through a.dot(b) and norm(a)*norm(b).

    Due to the Redis implementation, the key must be split at [-1], which
    allows for the use of a hashmap rather than a key-value pair. This
    implementation limits Redis overhead."""
    import numpy as np
    ## call the Singleton implementation of Redis
    r = connection()
    pipe = r.pipeline()
    # change code to be ready for chunks
    keys = key.split(',')
    
    # pipeline the key acquisition 
    for key in keys:
        key_front = key[:-1]
        key_back = key[-1:]
        pipe.hget(key_front, key_back+':i')
        pipe.hget(key_front, key_back+':v')
        # pull the data, decompress it, and change the data type
        #inds = np.array(np.frombuffer(zlib.decompress(r.hget(key_front, key_back+':i')),dtype=np.int32))
        #vals = np.array(np.frombuffer(zlib.decompress(r.hget(key_front, key_back+':v')),dtype=np.float16)).astype(np.float64)
        # extract the indives and values from the target SparseVector
        #inds2 = np.array(compare.indices)
        #vals2 = np.array(compare.values)
        #score = cos(inds,vals,inds2,vals2)
        # limit the number of points written to the database
#        if score > 0.05 and limit:
    values = pipe.execute()
    #print(values)
    target_inds = np.array(compare.indices)
    target_vals = np.array(compare.values)
    inds = values[::2]
    vals = values[1::2]
    scores = [(0.0,'blank')]
    data_store = np.zeros((300,2),dtype=np.int32)
#    max = 0.0
    for ind, val, key in zip(inds, vals, keys):
        ind = np.array(np.frombuffer(zlib.decompress(ind),dtype=np.int32))
#        val = np.random.rand(150)
        val = np.frombuffer(zlib.decompress(val),dtype=np.float16).astype(np.float64)
#        ind = np.random.randint(0,104857,size=len(val)).astype(np.int32)
#        sv = SparseVector(1048576, ind, val)
#        sc = sv.dot(compare)/(compare.norm(2)*sv.norm(2))
        #sc = np.random.rand(1)[0]
        sc = cos(ind, val, target_inds, target_vals, data_store)
#        try:
#            c_inds = common_inds(ind, target_inds)
#        except:
#        print(list(ind), list(target_inds))
#        print(len(target_inds), len(ind)) 
#        try:
#            if len(target_inds) > len(ind):
#                c_inds = common_inds(ind,target_inds)
#                sc = cos_np(c_inds,val,target_vals)#
#
#            else:
#                c_inds = common_inds(target_inds, ind)
#                sc = cos_np(c_inds,target_vals,val)
#        except Exception, e:
#            print(e)
#            sc = 0.1
#            print(target_inds, ind)
#        sc = cos(val,target_vals)
#        try:
#            sc = cos(ind, val, target_inds, target_vals) #, data_store)
#        except:
#            print(e)
#            sc = 0.1
#            print(ind, target_inds)
        if sc > max(scores)[0] or len(scores) < 5:
            scores.append((sc, key))
        
#        scores.append((cos(ind, val, target_inds, target_vals),key))
#    print(len(scores))
    scores = sorted(scores, reverse=True)
#    print(scores[:5])
    # write the top 5
#    dd = dict()
    for score, key in scores[:5]:
#        print(score, key)
        pipe.zadd('temp0', {key:score})
    pipe.execute()
#        dd[key] = score
#    r.zadd('temp0', {key: score})
    #r.zadd('temp0', {key:score})
    #pipe.execute()
    return 1


def retrieve_keys(tags, common=True):
    """Given a list of tags, return the set of keys common to all the tags,
    if common is set to true.  Return the Union if it is set to false."""
    r = StrictRedis.from_url('redis://10.0.0.10:6379')
    # if tags exist, filter them (later)
    # print(tags)
    if tags == []:
        return []
    else:
        print('FILTERING')
        if common:
            available_keys = set([])
        else:
            available_keys = [set([]) for tag in tags]
        # implement union of sets
        for count, tag in enumerate(tags):
             try:
                 keys_list = r.get(tag.strip()).split(',')[1:]
                 for key in keys_list:
                     if common:
                         available_keys.add(key)
                     else:
                         available_keys[count].add(key)
             except:
                 print('Tag %s not found - check spelling' % tag)
    if not common:
        available_keys = set().intersection(*available_keys)
    return list(available_keys)

def handler(message):
    """The main function which is applied to the Kafka streaming session
    and iterates through each of the received messages.

    Ideally, this would eventually be parallelizable, which would
    only require minor modifications to the code that gets the
    key lists from redis, which can often contain 1M+ keys."""
    records = message.collect()
    list_collect = []
    for r in records:

        read = json.loads(r[1].decode('utf-8'))
        list_collect.append((read['text'],read['tags']))
        l1 = (clean(read['text']),read['tags'])
        job = read['index']

        # to do: clean input text
        data = spark.createDataFrame([l1],['cleaned_body','tags'])
        data = model.transform(data)
        d = data.select('features','tags').collect()
#        print(d)
        keys = retrieve_keys(d[0]['tags'])
        # look to optimize slice length based on keys and throughput
        slice_length = max(len(keys)//10000,min(len(keys)//49,200))
        print(slice_length)
#        slice_length = 1000
        keys2 = [','.join(keys[i:i+slice_length]) for i in range(0,len(keys),slice_length)]
        #keys2 = [str(keys[(i-1)*slice_length:i*slice_length]) for i in range(len(keys)//slice_length-1)]
        print(len(keys), len(keys2))
        keys = spark.createDataFrame(keys2, StringType())
        score_udf = udf(lambda r: get_features(r,d[0]['features']), FloatType())
        keys = keys.withColumn('features', score_udf(keys['value'])).collect()
        # need to get top result from zadd
        report_to_redis(job)

    return

model = PipelineModel.load(idfpath)
ssc = StreamingContext(sc, 1)

kafkaStream = KafkaUtils.createDirectStream(
    ssc,
    ['cloud'],
    {'metadata.broker.list':'10.0.0.11:9092'}
)

kafkaStream.foreachRDD(handler)
ssc.start()
ssc.awaitTermination(timeout=None)
