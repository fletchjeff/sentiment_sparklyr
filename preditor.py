from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegressionModel
import os

#      .config("spark.hadoop.yarn.resourcemanager.principal",os.getenv("HADOOP_USER_NAME"))\

## Note this a local Spark instance running in the engine
spark = SparkSession.builder \
      .appName("Sentiment") \
      .master("local[*]") \
      .config("spark.driver.memory","4g")\
      .config("spark.hadoop.yarn.resourcemanager.principal",os.getenv("HADOOP_USER_NAME"))\
      .getOrCreate()

s3_bucket = os.getenv("STORAGE")      

    
model = PipelineModel.load(s3_bucket + "/datalake/data/sentiment/lm_model_r") 

sentence_df = spark.createDataFrame([(input_sentence,)],['spoken_words'])

#  test_text_df <- as.data.frame(args$sentence)
#  colnames(test_text_df) <- "spoken_words"
#
#  sdf_copy_to(sc, test_text_df, name="test_text", overwrite = TRUE)
#  test_text <- tbl(sc, "test_text")
#
#  test_text_tokenized <- test_text %>%
#    mutate(spoken_words = regexp_replace(spoken_words, "[_\"\'():;,.!?\\-]", " ")) %>%
#    ft_tokenizer(input_col="spoken_words",output_col= "word_list") %>%
#    ft_stop_words_remover(input_col = "word_list", output_col = "wo_stop_words")
#
#  test_text_w2v <- ft_word2vec(test_text_tokenized, input_col = "wo_stop_words", output_col = "result", min_count = 0)
#
#  result <- ml_predict(reloaded_model,test_text_w2v)

args = {"sentence":"I'm no dunce, I was born an oaf and I'll die an oaf"}#"AA,ICT,DFW,1135,85,11,328"}

#def predict(args):
input_sentence = args["sentence"]#.split(",")
sentence_df = spark.createDataFrame([(input_sentence,)],['spoken_words'])
sentence_df = sentence_df.select(regexp_replace('spoken_words',r'[_\"\'():;,.!?\\-]', ' ').alias('spoken_words'))

tokenizer = Tokenizer(inputCol="spoken_words", outputCol="word_list")
sentence_df = tokenizer.transform(sentence_df)

remover = StopWordsRemover(inputCol="word_list", outputCol="wo_stop_words")
sentence_df = remover.transform(sentence_df)

from pyspark.ml.feature import Tokenizer, RegexTokenizer, StopWordsRemover
from pyspark.sql.functions import col, udf,regexp_replace
from pyspark.sql.types import IntegerType


from pyspark.ml.feature import Word2Vec


# Learn a mapping from words to Vectors.
word2Vec = Word2Vec(vectorSize=100, minCount=0, inputCol="wo_stop_words", outputCol="result")
w2v_model = word2Vec.fit(sentence_df)

result = w2v_model.transform(sentence_df)
for row in result.collect():
    text, vector = row
    print("Text: [%s] => \nVector: %s\n" % (", ".join(text), str(vector)))


  features = features.withColumn('CRS_DEP_HOUR', substring(convert_time_to_hour("CRS_DEP_TIME"),0,2).cast('float'))
  result = model.transform(features).collect()[0].prediction
  return {"result" : result}