library(sparklyr)
library(dplyr)
library(jsonlite)

config <- spark_config()
config$spark.hadoop.yarn.resourcemanager.principal <- Sys.getenv("HADOOP_USER_NAME")
config$spark.driver.memory <- "4g"
sc <- spark_connect(master = "local", config=config)

storage <- Sys.getenv("STORAGE")      

reloaded_model <- ml_load(sc, paste(Sys.getenv("STORAGE"),"/datalake/data/sentiment/sentiment_model_r",sep=""))


# s3_bucket = os.getenv("STORAGE")      
# 
# tokenizer = Tokenizer(inputCol="spoken_words", outputCol="word_list")
# remover = StopWordsRemover(inputCol="word_list", outputCol="wo_stop_words")
# w2v_model_fitted = Word2VecModel.load(s3_bucket + "/datalake/data/sentiment/w2v_model_fitted")
# lr_model = PipelineModel.load(s3_bucket + "/datalake/data/sentiment/lr_model")
# 
# #args = {"sentence":"I'm no dunce, I was born an oaf and I'll die an oaf"}
# 
# def predict(args):
#   input_sentence = args["sentence"]#.split(",")
# sentence_df = spark.createDataFrame([(input_sentence,)],['spoken_words'])
# sentence_df = sentence_df.select(regexp_replace('spoken_words',r'[_\"\'():;,.!?\\-]', ' ').alias('spoken_words'))
# sentence_df = tokenizer.transform(sentence_df)
# sentence_df = remover.transform(sentence_df)
# sentence_df = w2v_model_fitted.transform(sentence_df)
# result = lr_model.transform(sentence_df).collect()[0]
# #result.prediction
# if result.prediction==0:
#   sentiment = 'Negative'
# conf = round(result.probability[0] * 100,3)
# else:
#   sentiment = 'Positive'  
# conf = round(result.probability[1] * 100,3)
# return {"sentiment" : sentiment, "confidence" : conf }


#args <- fromJSON('{"sentence":"Im no dunce. I was born an oaf and Ill die an oaf"}')

predict_sentiment <- function(args) {
  test_text_df <- as.data.frame(args$sentence)
  colnames(test_text_df) <- "spoken_words"

  sdf_copy_to(sc, test_text_df, name="test_text", overwrite = TRUE)
  test_text <- tbl(sc, "test_text")
  
  test_text_regex <- test_text %>%
    mutate(spoken_words = regexp_replace(spoken_words, "[_\"\'():;,.!?\\-]", " "))

  result <- ml_transform(reloaded_model,test_text_regex)

  list("result" = as.data.frame(result)$prediction)
}
