library(sparklyr)
library(dplyr)
library(jsonlite)

config <- spark_config()
config$spark.yarn.access.hadoopFileSystems <- Sys.getenv("STORAGE")
config$spark.driver.memory <- "4g"

sc <- spark_connect(master = "local", config=config)

reloaded_model <- ml_load(sc, paste(Sys.getenv("STORAGE"),"/datalake/data/sentiment/lm_model_r",sep=""))

#args <- fromJSON('{"sentence":"Im no dunce"}')

predict_sentiment <- function(args) {
  test_text_df <- as.data.frame(args$sentence)
  colnames(test_text_df) <- "spoken_words"

  sdf_copy_to(sc, test_text_df, name="test_text", overwrite = TRUE)
  test_text <- tbl(sc, "test_text")

  test_text_tokenized <- test_text %>%
    mutate(spoken_words = regexp_replace(spoken_words, "[_\"\'():;,.!?\\-]", " ")) %>%
    ft_tokenizer(input_col="spoken_words",output_col= "word_list") %>%
    ft_stop_words_remover(input_col = "word_list", output_col = "wo_stop_words")

  test_text_w2v <- ft_word2vec(test_text_tokenized, input_col = "wo_stop_words", output_col = "result", min_count = 0)

  result <- ml_predict(reloaded_model,test_text_w2v)

  list("predicted_sentiment_value" = as.data.frame(result)$prediction)
}
