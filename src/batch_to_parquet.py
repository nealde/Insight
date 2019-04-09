from pyspark.sql import SparkSession, DataFrame
from pyspark.ml import Pipeline, Transformer
from pyspark.ml.feature import HashingTF, IDF, IDFModel, Tokenizer
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

sc = SparkSession.builder.appName("Text Pipeline All Data").getOrCreate()


parquetpath = 's3n://neal-dawson-elli-insight-data/models/b5'
idfpath = 's3n://neal-dawson-elli-insight-data/models/idf-model5'
datapath = "s3n://neal-dawson-elli-insight-data/insight/final3/questions/questions-000000000*.csv"

# CUSTOM TRANSFORMER ----------------------------------------------------------------
#class TextCleaner(Transformer):
#    """
#    A custom Transformer which drops all columns that have at least one of the
#    words from the banned_list in the name.
#    """#
#
#    def __init__(self, inputCol='body', outputCol='cleaned_body'):
#        super(TextCleaner, self).__init__()
#         self.banned_list = banned_list
#    def clean(line):
#        line = line.lower().replace("\n"," ").replace("\r","").replace(',',"").replace(">","> ").replace("<", " <")
#        return line
#    clean_udf = udf(lambda r: clean(r), StringType())
#
#    def _transform(self, df: DataFrame) -> DataFrame:
#        df = df.withColumn('cleaned_body', self.clean_udf(df['body']))
#        df = df.drop('body')
    #         df = df.drop(*[x for x in df.columns if any(y in x for y in self.banned_list)])
#        return df

def clean(line):
    line = line.lower().replace("\n"," ").replace("\r","").replace(',',"").replace(">","> ").replace("<", " <").replace("|"," ")
    return line

# apply UDF to allow for processing in DataFrames
clean_udf = udf(lambda r: clean(r), StringType())

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

# savev model
model.write().overwrite().save(idfpath)

processedData = model.transform(cleanData)
processedData.select('id','title','features','tags').write.parquet(parquetpath)
