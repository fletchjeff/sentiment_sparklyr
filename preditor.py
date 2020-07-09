from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import regexp_replace
import os

spark = SparkSession.builder \
      .appName("Sentiment") \
      .master("local[*]") \
      .config("spark.driver.memory","4g")\
      .config("spark.hadoop.yarn.resourcemanager.principal",os.getenv("HADOOP_USER_NAME"))\
      .getOrCreate()

s3_bucket = os.getenv("STORAGE")      
    
sentiment_model = PipelineModel.load(s3_bucket + "/datalake/data/sentiment/sentiment_model_r")

#args = {"sentence":"I'm no dunce, I was born an oaf and I'll die an oaf"}

def predict(args):
  input_sentence = args["sentence"]#.split(",")
  sentence_df = spark.createDataFrame([(input_sentence,)],['spoken_words'])
  sentence_df = sentence_df.select(regexp_replace('spoken_words',r'[_\"\'():;,.!?\\-]', ' ').alias('spoken_words'))
  result = sentiment_model.transform(sentence_df).collect()[0].prediction
  return {"result" : result}
