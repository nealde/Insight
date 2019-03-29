from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import Normalizer 
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix 

sc = SparkSession.builder.appName("TextProcess").getOrCreate()
# sc = SparkContext(appName = 'TextProcess').getOrCreate()
# def line(a):
#     sp = a.split(",")
#     return a.split("~")
# process('pq.csv')
def word_count(row):
    return len(row.split(" "))

# files = sc.textFile("pq_clean.csv") \
# .map(lambda line: line.split(",")[1]) \
# .cache()
files = sc.read.format("csv").option("header","false").load("pq_clean.csv")
f = files.take(10)




tk = Tokenizer(inputCol="_c1", outputCol="words")
words = tk.transform(files)
# w = words.collect()
hashingTF = HashingTF(inputCol="words", outputCol='rawFeatures')
tf = hashingTF.transform(words)

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(tf)
rescaledData = idfModel.transform(tf)
rescaledData.cache()

# tf = hashingTF.transform(words)
# tf.cache()
rescaledData.select("_c0", "features").show() 


normalizer = Normalizer(inputCol="features", outputCol="norm") 
data = normalizer.transform(rescaledData)

mat = IndexedRowMatrix(data.select("_c0", "norm").rdd.map(lambda row: IndexedRow(row._c0, row.norm.toArray()))).toBlockMatrix() 
dot = mat.multiply(mat.transpose())
dot.toLocalMatrix().toArray()
