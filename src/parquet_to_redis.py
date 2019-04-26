from pyspark.sql import SparkSession, DataFrame
from pyspark.ml import Pipeline, Transformer, PipelineModel
from pyspark.ml.feature import HashingTF, IDF, IDFModel, Tokenizer
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import StringType, FloatType, IntegerType
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
    pipe = r.pipeline()
    tags = row['tags']
    if tags.find("|") > 0:
        tags = tags.split('|')
    else:
        print(tags)
        tags = [tags]
    idd = row['id']
    title = row['title']
    embed = row['features']
    end_idd = idd[-1:]
    front_idd = idd[:-1]

    # compress and store inds and values
    inds = zlib.compress(embed.indices.tobytes())
    vals = zlib.compress(embed.values.astype('float16').tobytes())

    pipe.hset('id:'+front_idd,end_idd+':t',title)
    pipe.hset('id:'+front_idd,end_idd+':s',str(embed.size))
    pipe.hset('id:'+front_idd,end_idd+':i',inds)
    pipe.hset('id:'+front_idd,end_idd+':v',vals)

    for tag in tags:
#         r.hset(tag+':'+idd[:2],idd[2:],1)
         pipe.append(tag.strip(), ",id:"+idd)
    pipe.execute()
    return 1
#    return to_write

redis_udf = udf(lambda row: store_redis(row), IntegerType())
dd = sc.read.parquet(parquetpath).repartition(1000)
#dd.select('features').show(20)
# run this to store data into redis
#dd.withColumn('tw',redis_udf(struct([dd[x] for x in dd.columns]))).select('id','tw')\
#.collect()

def redis_important_vectors(row):
    r = connection()
    pipe = r.pipeline()
    idd = ',id:'+row['id']
    embed = row['features']
    inds = embed.indices
    vals = embed.values
    top_inds = [y for _, y in sorted(zip(vals,inds), reverse=True)[:5]]
    for ind in top_inds:
        pipe.append('inds:'+str(ind), idd)
    pipe.execute()
    return 1

vectors_udf = udf(lambda row: redis_important_vectors(row), IntegerType())
dd.withColumn('tw',vectors_udf(struct([dd[x] for x in dd.columns]))).select('id','tw')\
.collect()



#.select('tw').collect()
#dd.createOrReplaceTempView("table1").cache()
#df2 = spark.sql("SELECT field1 AS f1, field2 as f2 from table1 limit 100")
#df2.collect()
