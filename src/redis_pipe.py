from pyspark.sql import SparkSession, DataFrame
from pyspark.ml import Pipeline, Transformer, PipelineModel
from pyspark.ml.feature import HashingTF, IDF, IDFModel, Tokenizer
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from redis import StrictRedis

sc = SparkSession.builder.appName("Batch Process In Pipeline").getOrCreate()

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
    line = line.lower().replace("\n"," ").replace("\r","").replace(',',"").replace(">","> ").replace("<", " <")
    return line
clean_udf = udf(lambda r: clean(r), StringType())

def store_redis(row):
    redis_host = '10.0.0.7'
    redis_port = 6379
    redis_password = 'AhrIykRVjO9GHA52kmYou7iUrsDbzJL+/7vjeTYhsLmpskyAY8tnucf4QJ7FpvVzFNNKuIZVVkh1LRxF'
    r = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
    try:
        tags = row['tags']
        if tags.find("|") > 0:
            tags = tags.split('|')
    except:
        print(tags)
        return 0
    idd = row['id']
    title = row['title']
    creation = row['creation_date']
    embed = row['features']
    to_write = "|".join([str(embed.size), str(list(embed.indices)),str(list(embed.values))])
    r.set('id:'+idd, title+'|'+to_write+'|'+creation)
    #tags = row['tags']
    for tag in tags:
#        r.append(tag, ","+idd)
        curr = r.get(tag)
        if curr is None:
            r.set(tag, idd)
        else:
            r.set(tag, curr+","+idd)
    return 1

dirtyData = sc.read\
    .csv("s3n://neal-dawson-elli-insight-data/insight/final3/questions/questions-00000000000*.csv", header=True, multiLine=True, escape='"')\
    .repartition(400)\
    .select('id','title','body','tags','creation_date')

dirtyData = dirtyData.withColumn('cleaned_body', clean_udf(dirtyData['body']))\
    .drop('body')
#dirtyData.cache()
#dirtyData = dirtyData.select('id','title','cleaned_body','tags','accepted_answer_id')
#dirtyData.select('cleaned_body').show()

#cleaner = TextCleaner(inputCol='body', outputCol='cleaned_body')
#dirtyData = cleaner.transform(dirtyData)

#tokenizer = Tokenizer(inputCol="cleaned_body", outputCol="words")
#hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=2**20)
#idf = IDF(inputCol="rawFeatures", outputCol="features")
#model = PipelineModel(stages=[tokenizer, hashingTF, idf])

# Fit the pipeline to training documents.
#model = pipeline.fit(dirtyData)

idfpath = 's3n://neal-dawson-elli-insight-data/models/idf-model3'
parquetpath = 's3n://neal-dawson-elli-insight-data/models/b4'

model = PipelineModel.load(idfpath)
#model.load(idfpath)
#model.write().overwrite().save(idfpath)
dirtyData = model.transform(dirtyData)\
    .select('id','title','features','creation_date','tags')\
    .cache()

dirtyData.write.parquet(parquetpath)

dd = dirtyData.rdd.map(store_redis).sum()
print(" ")
print(" ")
print(dd)
#dd.collect()


#idftr.save(idfpath)
#try:
  #  old_data = sc.read.parquet(parquetpath)
 #   dirtyData.union(old_data)
    #SaveMode.Overwrite
#    dirtyData.select('id','title','cleaned_body','features','tags').write.mode(SaveMode.Overwrite).parquet(parquetpath)

# pyspark.sql.analysisexception
#except:
#    print("file not found - continuing")
#dirtyData.select('id','title','cleaned_body','features','tags').write.overwrite.parquet(parquetpath)
#dirtyData.select('id','title','cleaned_body','features','tags','accepted_answer_id').write.overwrite().save.parquet(parquetpath).parquet(parquetpath)

#loadedModel = IDFModel.load(idfpath)

#sentenceData = spark.createDataFrame([(0.0, text),],['label','sentence'])
#tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
#wordsData = tokenizer.transform(sentenceData)

#hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=2**12)
#featurizedData = hashingTF.transform(wordsData)
# alternatively, CountVectorizer can also be used to get term frequency vectors
#idfPath = 'idf/'

#modelPath = "temp/idf-model"
# model.save(modelPath)
#loadedModel = IDFModel.load(modelPath)
#sample = loadedModel.transform(featurizedData).take(1)[0]['features']

#data = spark.read.format("parquet").load(parquetpath)
# tf = hashingTF.transform(words)
# tf.cache()

#rescaledData.select("_c0", "features").show()

#rescaledData.write.parquet(parquetpath)

#sample = rescaledData.select('_c0','features').take(2)[1]['features']

#def cos(a, b):
#     print(a[0].norm(2))
#    return a[0].dot(b)/(a[0].norm(2)*b.norm(2))

#sim = rescaledData.select("features").rdd.map(lambda x: cos(x, sample))
# sim = rescaledData.select("features").rdd.map(lambda x: cos(x, sample)).sortBy(lambda x: -x).take(2)


#topFive = sorted(enumerate(sim.collect()), key= lambda kv: -kv[1])[0:5]
#for idx, val in topFive:
#    print("doc '%s' has score %.4f" % (idx, val))

#normalizer = Normalizer(inputCol="features", outputCol="norm")
#data = normalizer.transform(rescaledData)

#mat = IndexedRowMatrix(data.select("_c0", "norm").rdd.map(lambda row: IndexedRow(row._c0, row.norm.toArray()))).toBlockMatrix()
#dot = mat.multiply(mat.transpose())
#dot.toLocalMatrix().toArray()
