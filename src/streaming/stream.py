import json
import numpy as np
import os
import sys
import zlib


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell'
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/cython")

# Spark
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DataType, FloatType
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.linalg import SparseVector, VectorUDT

#    Spark Streaming
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from redis import StrictRedis

## local cosine similarity in Cython
from cy_utils import cos
from config import IDF_PATH, REDIS_URL


def connection():
    """Singleton implementation of a Redis connection, which significantly
    speeds up bulk writes and avoids address collisions.
    """
    global _connection
    if _connection is None:
        _connection = StrictRedis.from_url(REDIS_URL)
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

    # pipeline the key acquisition -
    # Pipelining lets Redis send multiple commands at once,
    # significantly reducing overhead
    for key in keys:
        key_front = key[:-1]
        key_back = key[-1:]
        pipe.hget(key_front, key_back+':i')
        pipe.hget(key_front, key_back+':v')
    values = pipe.execute()
    target_inds = np.array(compare.indices)
    target_vals = np.array(compare.values)
    inds = values[::2]
    vals = values[1::2]
    scores = [(0.0,'blank')]
    # placeholder array for cython code
    data_store = np.zeros((300,2),dtype=np.int32)

    for ind, val, key in zip(inds, vals, keys):
        ind = np.array(np.frombuffer(zlib.decompress(ind),dtype=np.int32))
        val = np.frombuffer(zlib.decompress(val),dtype=np.float16).astype(np.float64)
        sc = cos(ind, val, target_inds, target_vals, data_store)
        if sc > max(scores)[0] or len(scores) < 5:
            scores.append((sc, key))

    scores = sorted(scores, reverse=True)
    # top 5 scores globally requires top 5 scores locally
    for score, key in scores[:5]:
        pipe.zadd('temp0', {key:score})
    pipe.execute()
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
    for record in records:
        # Parse record
        read = json.loads(record[1].decode('utf-8'))
        list_collect.append((read['text'],read['tags']))
        data = (clean(read['text']),read['tags'])
        job = read['index']

        data = spark.createDataFrame([data],['cleaned_body','tags'])
        data = model.transform(data)
        d = data.select('features','tags').collect()

        keys = retrieve_keys(d[0]['tags'])
        # look to optimize slice length based on keys and throughput
        slice_length = max(len(keys)//10000,min(len(keys)//49,200))
        print(slice_length)
        keys_sliced = [','.join(keys[i:i+slice_length]) for i in range(0,len(keys),slice_length)]
        keys = spark.createDataFrame(keys_sliced, StringType())
        score_udf = udf(lambda r: get_features(r,d[0]['features']), FloatType())
        keys = keys.withColumn('features', score_udf(keys['value'])).collect()
        # need to get top result from zadd
        report_to_redis(job)
    return

def stream():

    idfpath = IDF_PATH

    spark = SparkSession\
         .builder\
         .appName("Streaming")\
         .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    _connection = None

    model = PipelineModel.load(idfpath)
    ssc = StreamingContext(sc, 0.1)

    kafkaStream = KafkaUtils.createDirectStream(
        ssc,
        ['cloud'],
        {'metadata.broker.list':'10.0.0.11:9092'}
    )

    kafkaStream.foreachRDD(handler)
    ssc.start()
    ssc.awaitTermination(timeout=None)

if __name__ == "__main__":
    stream()


