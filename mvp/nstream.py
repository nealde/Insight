#import kafka

# connect to kafka cluster
#cluster= kafka.KafkaClient("10.0.0.7:9092")
#topic="cloud"
#consumer_group = 'default_group'
#consumer = kafka.SimpleConsumer(cluster, consumer_group, topic)

# consume all messages
#for raw in consumer:
#    msg = raw.message.value
#    print(msg)
import os
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark/spark-streaming-kafka-0-10_2.11:2.3.1 pyspark-shell'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell'
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DataType, FloatType
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, Transformer, PipelineModel
from pyspark.ml.linalg import SparseVector, VectorUDT
#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from redis import StrictRedis
#import redis
import numpy as np
from cy_utils import cos
import zlib
import json
#from rdistrib import norm

idfpath = 's3n://neal-dawson-elli-insight-data/models/idf-model3'
#model = PipelineModel.load(idfpath)
spark = SparkSession\
     .builder\
     .appName("streaming")\
     .getOrCreate()

sc = spark.sparkContext
#sc = SparkContext(appName="streaming")
sc.setLogLevel("WARN")


_connection = None
#global _connection

def connection():
    """Return the Redis connection to the URL given by the environment
    variable REDIS_URL, creating it if necessary.

    """
    from numpy.linalg import norm
    global norm
    global _connection
    if _connection is None:
        _connection = StrictRedis.from_url('redis://10.0.0.10:6379')
    return _connection

model = PipelineModel.load(idfpath)
ssc = StreamingContext(sc, 1)

#kafkaStream = KafkaUtils.createStream(ssc, 'cdh57-01-node-01.moffatt.me:2181', 'spark-streaming', {'twitter':1})
kafkaStream = KafkaUtils.createDirectStream(
    ssc,
    ['cloud'],
    {'metadata.broker.list':'10.0.0.11:9092'}
)


from numba import jit
#from numpy.linalg import norm
#global norm

def cos_np(inds1,vals1,inds2,vals2):
    i1 = np.where(np.isin(inds1,inds2))
    i2 = np.where(np.isin(inds2,inds1))
    product = np.sum(vals1[i1]*vals2[i2])
    return product/np.linalg.norm(vals1)/np.linalg.norm(vals2)

#@jit
#def cos2(inds1, vals1, inds2, vals2):
    #inds1 = np.array(inds1)
    #inds2 = np.array(inds2)
#    product = 0.0
#    count = 0
#    for count1 in range(len(inds1)):
#        for count2 in range(len(inds2)):
#            if inds1[count1] == inds2[count2]:
#                product += vals1[count1]*vals2[count2]
#    product /= np.linalg.norm(vals1, ord=2)*np.linalg.norm(vals2, ord=2)
#    return product

#def cos(a,b):
#    if a is None:
#        return 0.0
#    return float(a.dot(b)/(a.norm(2)*b.norm(2)))

#cos_udf(b) = udf(lambda x: cos(x, b), FloatType())
def report_to_redis(job):
    #redis_host = '10.0.0.10'
    #redis_port = 6379
    #redis_password = 'AhrIykRVjO9GHA52kmYou7iUrsDbzJL+/7vjeTYhsLmpskyAY8tnucf4QJ7FpvVzFNNKuIZVVkh1LRxF'
    #r = connection()
    #print(results)
    r = StrictRedis.from_url("redis://10.0.0.10:6379") #, password=redis_password)
    for i in range(5):
        res = r.zpopmax('temp1')
        print(res)
        #key_front = res[0][:-1]
        #key_back = key[-1:]

        title = r.hget(res[0][0][:-1],res[0][0][-1:]+':t')
        r.set('success:'+str(job)+'|'+str(i), res[0][0]+'|%1.3f'%res[0][1])
    r.delete('temp1')
    return 0


def get_features(key, compare):
    import numpy as np
    #redis_host = '10.0.0.10'
    #redis_port = 6379
    #redis_password = 'AhrIykRVjO9GHA52kmYou7iUrsDbzJL+/7vjeTYhsLmpskyAY8tnucf4QJ7FpvVzFNNKuIZVVkh1LRxF'
#    connection()
#    r = _connection
    r = connection()
    #try:
#    r = StrictRedis.from_url("redis://10.0.0.10:6379") #, password=redis_password)
    #except ConnectionError:
    #    print('failed to connect')
    #    return None
    #print('key', key)
    #print(key)
    key_front = key[:-1]
    key_back = key[-1:]
    #print(key_front, key_back)
    #title = r.hget(key_front, key_back+':t')
    size = r.hget(key_front, key_back+':s')
#    print(size)
    inds = np.array(np.frombuffer(zlib.decompress(r.hget(key_front, key_back+':i')),dtype=np.int32))
    vals = np.array(np.frombuffer(zlib.decompress(r.hget(key_front, key_back+':v')),dtype=np.float16)).astype(np.float64)
    inds2 = np.array(compare.indices)
    vals2 = np.array(compare.values)
    #print(inds, vals, inds2, vals2)
    score = cos(inds,vals,inds2,vals2)
#    return "15"
    #return str(size)
#    print(inds, vals)
#    score = np.random.rand(1)
#    vector = SparseVector(size, inds, vals)
#    score = vector.dot(compare)/(vector.norm(2)*compare.norm(2))
#    print(score)
    if score > 0.1:
        r.zadd('temp1', {key:score})
    return 1
#    return SparseVector(size, inds, vals)


    #read = r.get(key).split('|')
#    print(read[0], read[1])
#    print(len(read))
    #try:
    #    title = read[0]
    #    size = int(read[1])
    #    inds = np.frombytes(zlib.decompress(read
        #inds = np.fromstring(read[2][1:-1], sep=',')
        #vals = np.fromstring(read[3][1:-2],sep=',')
        #print('len', size, len(inds), len(vals))
        #print('max', size, inds.max(), vals.max())
    #    return SparseVector(size, inds, vals)
    #except:
    #    print('failed', key)
    #    return None


#get_features_udf = udf(lambda r: get_features(r), VectorUDT())
#get_features_udf = udf(lambda r: get_features(r), StringType())

def retrieve_keys(tags):
    #redis_host = '10.0.0.10'
    #redis_port = 6379
    #redis_password = 'AhrIykRVjO9GHA52kmYou7iUrsDbzJL+/7vjeTYhsLmpskyAY8tnucf4QJ7FpvVzFNNKuIZVVkh1LRxF'
 #   r = connection()
    r = StrictRedis.from_url('redis://10.0.0.10:6379') #, password=redis_password)
    # if tags exist, filter them (later)
    print(tags)
    if tags == []:
        return []
#        available_keys = r.keys('id:*') # this is no longer safe
#        for key in available_keys:
        
    else:
        print('FILTERING')
#        a_keys = [r.get(tag).split(',')[1:] for tag in tags]
        
        available_keys = set([])
        for tag in tags:
             try:
                 keys_list = r.get(tag).split(',')[1:]
                 for key in keys_list:
                     available_keys.add(key)
             except:
                 print('Tag %s not found - check spelling' % tag) 
    # eventually wont need
#    available_keys = ["id:"+key for key in available_keys]
    return available_keys
#data = []
    #for key in available_keys:
    #    data.append((r.get(key).split('|')))
    #return available_keys
    #print(available_keys)
    #return available_keys

#collected = kafkaStream.collect()
#kafkaStream.foreachRDD(handler)
def handler(message):
    records = message.collect()
    list_collect = []
    for r in records:
        #print(r[1])
        read = json.loads(r[1].decode('utf-8'))
        list_collect.append((read['text'],read['tags']))
        l1 = (read['text'],read['tags'])
        job = read['index']
    #print(list_collect)
        # to do: clean input text
        data = spark.createDataFrame([l1],['cleaned_body','tags'])
        data = model.transform(data)
        d = data.select('features','tags').collect()
        print(d[0]['features'])
        keys = retrieve_keys(d[0]['tags'])
        print(len(keys))
        keys = spark.createDataFrame(keys, StringType())
#        keys.show(20)
        score_udf = udf(lambda r: get_features(r,d[0]['features']), FloatType())
        keys = keys.withColumn('features', score_udf(keys['value'])).collect()
        # need to get top result from zadd 
        #cos_udf = udf(lambda r: cos(r, d[0]['features']), FloatType())
        #keys = keys.withColumn('similarity', cos_udf(keys['features'])).sort('similarity', ascending=False)
#        keys.show(10)
#        top_result = keys.take(5)
#        print(top_result)
        report_to_redis(job)
        
        #print(d)
    #print(read)
    #print(records)
    return

kafkaStream.foreachRDD(handler)
ssc.start()
ssc.awaitTermination(timeout=None)
#print(collected)
