from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.feature import Word2VecModel, Tokenizer, StopWordsRemover
from pyspark.sql.functions import regexp_replace
import os

spark = SparkSession.builder \
      .appName("Sentiment") \
      .master("local[*]") \
      .config("spark.driver.memory","4g")\
      .config("spark.hadoop.yarn.resourcemanager.principal",os.getenv("HADOOP_USER_NAME"))\
      .getOrCreate()

storage = os.getenv("STORAGE")      

tokenizer = Tokenizer(inputCol="spoken_words", outputCol="word_list")
remover = StopWordsRemover(inputCol="word_list", outputCol="wo_stop_words")
w2v_model_fitted = Word2VecModel.load(storage + "/datalake/data/sentiment/w2v_model_fitted")
lr_model = PipelineModel.load(storage + "/datalake/data/sentiment/lr_model")

#args = {"sentence":"I'm no dunce, I was born an oaf and I'll die an oaf"}

def predict_sentiment(args):
  input_sentence = args["sentence"]#.split(",")
  sentence_df = spark.createDataFrame([(input_sentence,)],['spoken_words'])
  sentence_df = sentence_df.select(regexp_replace('spoken_words',r'[_\"\'():;,.!?\\-]', ' ').alias('spoken_words'))
  sentence_df = tokenizer.transform(sentence_df)
  sentence_df = remover.transform(sentence_df)
  sentence_df = w2v_model_fitted.transform(sentence_df)
  result = lr_model.transform(sentence_df).collect()[0]
  #result.prediction
  if result.prediction==0:
    sentiment = 'Negative'
    conf = round(result.probability[0] * 100,3)
  else:
    sentiment = 'Positive'  
    conf = round(result.probability[1] * 100,3)
  return {"sentiment" : sentiment, "confidence" : conf }

