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
import json

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
import redis
#from rdistrib import *

idfpath = 's3n://neal-dawson-elli-insight-data/models/idf-model'
#model = PipelineModel.load(idfpath)
spark = SparkSession\
     .builder\
     .appName("streaming")\
     .getOrCreate()

sc = spark.sparkContext
#sc = SparkContext(appName="streaming")
sc.setLogLevel("WARN")

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
    redis_host = '10.0.0.7'
    redis_port = 6379
    redis_password = 'AhrIykRVjO9GHA52kmYou7iUrsDbzJL+/7vjeTYhsLmpskyAY8tnucf4QJ7FpvVzFNNKuIZVVkh1LRxF'
    r = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
    for i, res in enumerate(results):
        r.set('success:'+str(job)+'|'+str(i), res['value']+'|{:f}'.format(res['similarity']))
    return 


def get_features(key):
    import numpy as np
    redis_host = '10.0.0.7'
    redis_port = 6379
    redis_password = 'AhrIykRVjO9GHA52kmYou7iUrsDbzJL+/7vjeTYhsLmpskyAY8tnucf4QJ7FpvVzFNNKuIZVVkh1LRxF'
    
    try:
        r = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
    except ConnectionError:
        print('failed to connect')
        return None
    #print('key', key)
    read = r.get(key).split('|')
#    print(read[0], read[1])
#    print(len(read))
    try:
        title = read[0]
        size = int(read[1])
        inds = np.fromstring(read[2][1:-1], sep=',')
        vals = np.fromstring(read[3][1:-2],sep=',')
        #print('len', size, len(inds), len(vals))
        #print('max', size, inds.max(), vals.max())
        return SparseVector(size, inds, vals)
    except:
        print('failed', key)
        return None

get_features_udf = udf(lambda r: get_features(r), VectorUDT())

def retrieve_keys(tags):
    redis_host = '10.0.0.7'
    redis_port = 6379
    redis_password = 'AhrIykRVjO9GHA52kmYou7iUrsDbzJL+/7vjeTYhsLmpskyAY8tnucf4QJ7FpvVzFNNKuIZVVkh1LRxF'
    r = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
    # if tags exist, filter them (later)
    print(tags)
    if tags == []:
        available_keys = r.keys('id:32*')
    else:
        print('FILTERING')
        a_keys = [r.get(tag).split(',') for tag in tags]
        available_keys = set([])
        for keys_list in a_keys:
             for key in keys_list:
                 available_keys.add(key)
    # eventually wont need
    available_keys = ["id:"+key for key in available_keys]
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
        data = spark.createDataFrame([l1],['cleaned_body','tags'])
        data = model.transform(data)
        d = data.select('features','tags').collect()
        print(d)
        keys = retrieve_keys(d[0]['tags'])
        keys = spark.createDataFrame(keys, StringType())
        keys = keys.withColumn('features',get_features_udf(keys['value']))
        cos_udf = udf(lambda r: cos(r, d[0]['features']), FloatType())
        keys = keys.withColumn('similarity', cos_udf(keys['features'])).sort('similarity', ascending=False)
        #keys.show(10)
        top_result = keys.take(5)
        print(top_result)
        report_to_redis(top_result, job)
        
        #print(d)
    #print(read)
    #print(records)
    return

kafkaStream.foreachRDD(handler)
ssc.start()
ssc.awaitTermination(timeout=None)
#print(collected)
