import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell'
import json

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, Transformer, PipelineModel
#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import redis
# redis_host = '10.0.0.7'
# redis_port = 6379
# redis_password = 'AhrIykRVjO9GHA52kmYou7iUrsDbzJL+/7vjeTYhsLmpskyAY8tnucf4QJ7FpvVzFNNKuIZVVkh1LRxF'
# r = redis.Redis(host=redis_host, port=redis_port, password=redis_password)

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

def str_to_sparse(str):
    from scipy import sparse


#collected = kafkaStream.collect()
#kafkaStream.foreachRDD(handler)
def retrieve_results(data_list, r):
    redis_host = '10.0.0.7'
    redis_port = 6379
    redis_password = 'AhrIykRVjO9GHA52kmYou7iUrsDbzJL+/7vjeTYhsLmpskyAY8tnucf4QJ7FpvVzFNNKuIZVVkh1LRxF'
    r = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
    # if tags exist, filter them (later)
    available_keys = r.keys('id*')
    print(available_keys[:10])
    return available_keys

def handler(message):
    records = message.collect()
    list_collect = []
    for r in records:
        #print(r[1])
        read = json.loads(r[1].decode('utf-8'))
        list_collect.append((read['text'],read['tags']))
        l1 = [read['text'],read['tags']]
    #print(list_collect)
        data = spark.createDataFrame(l1,['cleaned_body','tags'])
        data = model.transform(data)
        d = data.select('features').collect()
        retrieve_results(d, r)
        # print(d)
    #print(read)
    #print(records)
    return

kafkaStream.foreachRDD(handler)
ssc.start()
ssc.awaitTermination(timeout=180)
#print(collected)
