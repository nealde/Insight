import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/cython")

from pyspark.sql import SparkSession, DataFrame
from pyspark.ml import Pipeline, Transformer
from pyspark.ml.feature import HashingTF, IDF, IDFModel, Tokenizer
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

from cy_utils import cos
from config import IDF_PATH, DATA_PATH, REDIS_URL


def connection():
    """Return the Redis connection to the URL given by the environment
    variable REDIS_URL, creating it if necessary.
    """
    global _connection
    if _connection is None:
        _connection = StrictRedis.from_url(REDIS_URL)
    return _connection


def clean(line):
    """Remove unwanted charcters from the input string, including newlines, new row, commas, and | characters. This makes downstream processing significantly easier."""
    line = line.lower().replace("\n"," ").replace("\r","").replace(',',"").replace(">","> ").replace("<", " <").replace("|"," ")
    return line

# apply UDF to allow for processing in DataFrames
clean_udf = udf(lambda r: clean(r), StringType())

def store_redis(row):
    """Given a row of data, break it up and store the tags, id, tile, and inds and vals of embedded representations into redis, row by row.  This is not pipelined, as it is not a bottleneck and passing multiple rows is difficult in Spark."""
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
         pipe.append(tag.strip(), ",id:"+idd)
    pipe.execute()
    return 1

def main():
    sc = SparkSession.builder.appName("Batch Process Text").getOrCreate()

    idfpath = IDF_PATH
    datapath = DATA_PATH
    _connection = None

    dirtyData = sc.read.csv(datapath, header=True, multiLine=True, escape='"')\
                  .select('id','title','body','tags')\
                  .repartition(1000)

    cleanData = dirtyData.withColumn('cleaned_body', clean_udf(dirtyData['body']))\
                         .drop('body')

    # cache the data for fit and transorming
    cleanData.cache()

    tokenizer = Tokenizer(inputCol="cleaned_body", outputCol="words")
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=2**20)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    pipeline = Pipeline(stages=[tokenizer, hashingTF, idf])

    # Fit the pipeline to training documents.
    model = pipeline.fit(cleanData)

    # save model
    model.write().overwrite().save(idfpath)

    processedData = model.transform(cleanData)


    redis_udf = udf(lambda row: store_redis(row), IntegerType())
    processedData.withColumn('tw',redis_udf(struct([processedData[x] for x in processedData.columns]))).select('id','tw')\
                 .collect()

if __name__ == "__main__":
    main()
