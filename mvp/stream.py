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
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, Transformer, PipelineModel
#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


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

#collected = kafkaStream.collect()
#kafkaStream.foreachRDD(handler)
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
        print(d)
    #print(read)
    #print(records)
    return

kafkaStream.foreachRDD(handler)
ssc.start()
ssc.awaitTermination(timeout=180)
#print(collected)
