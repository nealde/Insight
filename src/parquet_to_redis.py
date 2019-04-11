from pyspark.sql import SparkSession, DataFrame
from pyspark.ml import Pipeline, Transformer, PipelineModel
from pyspark.ml.feature import HashingTF, IDF, IDFModel, Tokenizer
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import StringType, FloatType
import numpy as np
from redis import StrictRedis
import zlib

sc =  SparkSession.builder.appName("Parquet to Redis").getOrCreate()

parquetpath = 's3n://neal-dawson-elli-insight-data/models/b5'


_connection = None

def connection():
    """Return the Redis connection to the URL given by the environment
    variable REDIS_URL, creating it if necessary.

    """
    global _connection
    if _connection is None:
        _connection = StrictRedis.from_url('redis://10.0.0.10:6379')
    return _connection

# CUSTOM TRANSFORMER ----------------------------------------------------------------
#class TextCleaner(Transformer):
#    """
#    A custom Transformer which drops all columns that have at least one of the
#    words from the banned_list in the name.
#    """

#    def __init__(self, inputCol='body', outputCol='cleaned_body'):
#        super(TextCleaner, self).__init__()
#         self.banned_list = banned_list
#    def clean(line):
#        line = line.lower().replace("\n"," ").replace("\r","").replace(',',"").replace(">","> ").replace("<", " <")
#        return line
#    clean_udf = udf(lambda r: clean(r), StringType())

#    def _transform(self, df: DataFrame) -> DataFrame:
#        df = df.withColumn('cleaned_body', self.clean_udf(df['body']))
#        df = df.drop('body')
    #         df = df.drop(*[x for x in df.columns if any(y in x for y in self.banned_list)])
#        return df


def store_redis(row):
    r = connection()
    tags = row['tags']
    if tags.find("|") > 0:
        tags = tags.split('|')
    idd = row['id']
    title = row['title']
    embed = row['features']
    end_idd = idd[-1:]
    front_idd = idd[:-1]

    # compress and store inds and values
    inds = zlib.compress(embed.indices.tobytes())
    vals = zlib.compress(embed.values.astype('float16').tobytes())

    r.hset('id:'+front_idd,end_idd+':t',title)
    r.hset('id:'+front_idd,end_idd+':s',str(embed.size))
    r.hset('id:'+front_idd,end_idd+':i',inds)
    r.hset('id:'+front_idd,end_idd+':v',vals)

    for tag in tags:
#         r.hset(tag+':'+idd[:2],idd[2:],1)
         r.append(tag.strip(), ",id:"+idd)
    return 1
#    return to_write

#redis_udf = udf(lambda row: store_redis(row), StringType())
dd = sc.read.parquet(parquetpath).repartition(1000)
dd.createOrReplaceTempView("table1").cache()
df2 = spark.sql("SELECT field1 AS f1, field2 as f2 from table1 limit 100")
df2.collect()
