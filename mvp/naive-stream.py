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
import zlib
import json
#from rdistrib import *

parquetpath = 's3n://neal-dawson-elli-insight-data/models/b5'
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


def cos(a,b):
    if a is None:
        return 0.0
    return float(a.dot(b)/(a.norm(2)*b.norm(2)))

#cos_udf(b) = udf(lambda x: cos(x, b), FloatType())
def report_to_redis(results, job):
    #redis_host = '10.0.0.10'
    #redis_port = 6379
    #redis_password = 'AhrIykRVjO9GHA52kmYou7iUrsDbzJL+/7vjeTYhsLmpskyAY8tnucf4QJ7FpvVzFNNKuIZVVkh1LRxF'
    #r = connection()
    r = StrictRedis.from_url("redis://10.0.0.10:6379") #, password=redis_password)
    for i, res in enumerate(results):
        r.set('success:'+str(job)+'|'+str(i), res['value']+'|%1.3f'%res['similarity'])
    return 


def get_features(key):
#    import numpy as np
    #redis_host = '10.0.0.10'
    #redis_port = 6379
    #redis_password = 'AhrIykRVjO9GHA52kmYou7iUrsDbzJL+/7vjeTYhsLmpskyAY8tnucf4QJ7FpvVzFNNKuIZVVkh1LRxF'
    connection()
    r = _connection
#    r = connection()
    #try:
#    r = StrictRedis.from_url("redis://10.0.0.10:6379") #, password=redis_password)
    #except ConnectionError:
    #    print('failed to connect')
    #    return None
    #print('key', key)
    #print(key)
    key_front = key[:-2]
    key_back = key[-2:]
    #print(key_front, key_back)
    #title = r.hget(key_front, key_back+':t')
    size = r.hget(key_front, key_back+':s')
#    print(size)
    inds = np.frombuffer(zlib.decompress(r.hget(key_front, key_back+':i')),dtype=np.int32)
    vals = np.frombuffer(zlib.decompress(r.hget(key_front, key_back+':v')),dtype=np.float32)
#    return "15"
    #return str(size)
#    print(inds, vals)
    return SparseVector(size, inds, vals)


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

get_features_udf = udf(lambda r: get_features(r), VectorUDT())
#get_features_udf = udf(lambda r: get_features(r), StringType())

def retrieve_keys(tags):
    #redis_host = '10.0.0.10'
    #redis_port = 6379
    #redis_password = 'AhrIykRVjO9GHA52kmYou7iUrsDbzJL+/7vjeTYhsLmpskyAY8tnucf4QJ7FpvVzFNNKuIZVVkh1LRxF'
    r = connection()
    #r = redis.Redis(host=redis_host, port=redis_port) #, password=redis_password)
    # if tags exist, filter them (later)
    print(tags)
    if tags == []:
        available_keys = r.keys('id:*') # this is no longer safe
#        for key in available_keys:
        
    else:
        print('FILTERING')
        a_keys = [r.get(tag).split(',')[1:] for tag in tags]
        available_keys = set([])
        for keys_list in a_keys:
             for key in keys_list:
                 available_keys.add(key)
    # eventually wont need
#    available_keys = ["id:"+key for key in available_keys]
    return available_keys
#data = []
    #for key in available_keys:
    #    data.append((r.get(key).split('|')))
    #return available_keys
    #print(available_keys)
    #return available_keys
dset = spark.read.parquet(parquetpath).repartition(700).cache()

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
        data = spark.createDataFrame([l1],['cleaned_body','tags'])
        data = model.transform(data)
        d = data.select('features','tags').collect()
        #print(d)
        #keys = retrieve_keys(d[0]['tags'])
        #print(len(keys))
        #keys = spark.createDataFrame(keys, StringType()).repartition(48)
        #keys.show(20)
        #keys = keys.withColumn('features',get_features_udf(keys['value']))
        
        cos_udf = udf(lambda r: cos(r, d[0]['features']), FloatType())
        keys = dset.withColumn('similarity', cos_udf(dset['features'])).sort('similarity', ascending=False)
#        keys.show(10)
        top_result = keys.take(5)
#        print(top_result)
        report_to_redis(top_result, job)
        
        #print(d)
    #print(read)
    #print(records)
    return

kafkaStream.foreachRDD(handler)
ssc.start()
ssc.awaitTermination(timeout=None)
#print(collected)
