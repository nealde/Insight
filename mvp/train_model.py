from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF, IDF, IDFModel, Tokenizer
#from pyspark.ml.feature import Normalizer 
#from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix 

sc = SparkSession.builder.appName("TextProcess").getOrCreate()

files = sc.read.format("csv").option("header","false")\
.load("s3n://neal-dawson-elli-insight-data/insight/posts_questions/pq000.csv")
#.load("s3n://neal-dawson-elli-insight-data/insight/final/pw_csmall.csv")
#.load("s3n://neal-dawson-elli-insight-data/insight/posts_questions/pq000.csv")
#f = files.take(10)

#text = "<p> i am trying to create a report to display a summary of the values of the columns for each row.   a basic analogy would an inventory listing.  say i have about 15 locations like 2a 2b 2c 3a 3b 3c etc.   each location has a variety of items and the items each have a specific set of common descriptions i.e. a rating of 1-9 boolean y or n another boolean y or n.  it looks something like this:</p>   <pre> <code> 2a   4       y       n 2a   5       y       y 2a   5       n       y 2a   6       n       n       ... 2b   4       n       y   2b   4       y       y       ...etc. </code> </pre>   <p> what i would like to produce is a list of locations and summary counts of each attribute:</p>   <pre> <code> location    1 2 3 4 5 6 7 8 9      y  n        y n      total 2a                1 2 1            2  2        2 2        4 2b                2                1  1        2          2 ... ___________________________________________________________ totals            3 2 1            3  3        4 2        6 </code> </pre>   <p> the query returns fields:  </p>   <pre> <code> location_cd string   desc_cd int  y_n_1 string  y_n_2 string </code> </pre>   <p> i have tried grouping by location but cannot get the summaries to work.   i tried putting it in a table but that would only take the original query.  i tried to create datasets for each unit and create variables in each one for each of the criteria but that hasn't worked yet either.  but maybe i am way off track and crosstabs would work better?  i tried that and got a total mess the first time.  maybe a bunch of subreports?</p>   <p> can someone point me in the correct direction please?    it seemed easy when i started out but now i am getting nowhere.  i can get the report to print out the raw data but all i need are totals for each column broken down out by location.  </p> "



tk = Tokenizer(inputCol="_c1", outputCol="words")
words = tk.transform(files)
# w = words.collect()
hashingTF = HashingTF(inputCol="words", outputCol='rawFeatures')
tf = hashingTF.transform(words)

idf = IDF(inputCol="rawFeatures", outputCol="features")
idftr = idf.fit(tf)
rescaledData = idftr.transform(tf)
rescaledData.cache()

idfpath = 's3n://neal-dawson-elli-insight-data/models/idf-model'
parquetpath = 's3n://neal-dawson-elli-insight-data/models/b1'

idftr.save(idfpath)
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

rescaledData.write.parquet(parquetpath)

sample = rescaledData.select('_c0','features').take(2)[1]['features']

def cos(a, b):
#     print(a[0].norm(2))
    return a[0].dot(b)/(a[0].norm(2)*b.norm(2))

sim = rescaledData.select("features").rdd.map(lambda x: cos(x, sample))
# sim = rescaledData.select("features").rdd.map(lambda x: cos(x, sample)).sortBy(lambda x: -x).take(2)


topFive = sorted(enumerate(sim.collect()), key= lambda kv: -kv[1])[0:5]
for idx, val in topFive:
    print("doc '%s' has score %.4f" % (idx, val))

#normalizer = Normalizer(inputCol="features", outputCol="norm") 
#data = normalizer.transform(rescaledData)

#mat = IndexedRowMatrix(data.select("_c0", "norm").rdd.map(lambda row: IndexedRow(row._c0, row.norm.toArray()))).toBlockMatrix() 
#dot = mat.multiply(mat.transpose())
#dot.toLocalMatrix().toArray()
