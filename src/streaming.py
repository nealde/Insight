
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell'
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DataType, FloatType
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
#from pyspark.ml.linalg import SparseVector, VectorUDT

#    Spark Streaming
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from redis import StrictRedis
import numpy as np
import zlib
import json

# local cosine similarity in Cython
from cy_utils import cos


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
        res = r.zpopmax('temp'+job)
        title = r.hget(res[0][0][:-1],res[0][0][-1:]+':t')
        r.set('success:'+str(job)+'|'+str(i), res[0][0]+'|%1.3f'%res[0][1])
    r.delete('temp1')
    return 0


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
    key_front = key[:-1]
    key_back = key[-1:]
    # pull the data, decompress it, and change the data type
    inds = np.array(np.frombuffer(zlib.decompress(r.hget(key_front, key_back+':i')),dtype=np.int32))
    vals = np.array(np.frombuffer(zlib.decompress(r.hget(key_front, key_back+':v')),dtype=np.float16)).astype(np.float64)
    # extract the indives and values from the target SparseVector
    inds2 = np.array(compare.indices)
    vals2 = np.array(compare.values)
    score = cos(inds,vals,inds2,vals2)
    # limit the number of points written to the database
    if score > 0.1 && limit:
        r.zadd('temp1', {key:score})
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

        available_keys = set([])
        # implement union of sets
        for tag in tags:
             try:
                 keys_list = r.get(trim(tag)).split(',')[1:]
                 for key in keys_list:
                     available_keys.add(key)
             except:
                 print('Tag %s not found - check spelling' % tag)
    return available_keys

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

        keys = retrieve_keys(d[0]['tags'])
        print(len(keys))
        keys = spark.createDataFrame(keys, StringType())

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
