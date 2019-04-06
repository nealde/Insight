from pyspark.sql import SparkSession, DataFrame
from pyspark.ml import Pipeline, Transformer, PipelineModel
from pyspark.ml.feature import HashingTF, IDF, IDFModel, Tokenizer
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
#from pyspark.sql.redis import 
#from spark.sql import redis
import pyspark
#print(dir(pyspark.sql.dataframe))
#from pyspark.redis import *
#import redis

import os

from redis import Redis

_connection = None

def connection():
    """Return the Redis connection to the URL given by the environment
    variable REDIS_URL, creating it if necessary.

    """
    global _connection
    if _connection is None:
        _connection = Redis(host='10.0.0.7', port='6379')
    return _connection


spark =  SparkSession.builder.appName("Parquet to Redis").getOrCreate()
sc = spark.sparkContext
print(dir(sc))
#full_df = spark.read.csv("../../pantheon.tsv", sep="\t", quote="", header=True, inferSchema=True)

#print(full_df.dtypes)
#data = full_df.select("en_curid", "countryCode", "occupation")
#data.show(2)
#data.write.format("org.apache.spark.sql.redis").option("table", "people").option("key.column", "en_curid").save()
#df = spark.read.option("org.apache.spark.sql.redis").option("table", "id").option("key.column", "id").load()
df = sc.fromRedisKeyPattern('id:3256*',3).collect()
print(df)
