library(sparklyr)
library(dplyr)
library(jsonlite)

config <- spark_config()
config$spark.yarn.access.hadoopFileSystems <- Sys.getenv("STORAGE")
config$spark.driver.memory <- "4g"

sc <- spark_connect(master = "local", config=config)

reloaded_model <- ml_load(sc, paste(Sys.getenv("STORAGE"),"/datalake/data/sentiment/sentiment_model_r",sep=""))

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
