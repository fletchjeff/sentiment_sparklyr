## Who is the happiest Simpsons Character?

# Get the data set from here: https://www.kaggle.com/pierremegret/dialogue-lines-of-the-simpsons
# !wget https://raw.githubusercontent.com/laugustyniak/textlytics/master/textlytics/data/lexicons/AFINN-en-165.txt
# !hdfs dfs -copyFromLocal AFINN-en-165.txt $STORAGE/datalake/data/sentiment/

#install.packages("dplyr")
#install.packages("tibble")
#install.packages("sparklyr")
#install.packages("ggplot2")


#### Load the libraries
library(sparklyr)
library(dplyr)
library(ggplot2)

#### Create the Spark connection
storage <- Sys.getenv("STORAGE")

config <- spark_config()
config$spark.executor.memory <- "4g"
config$spark.executor.instances <- "3"
config$spark.executor.cores <- "4"
config$spark.driver.memory <- "2g"
config$spark.yarn.access.hadoopFileSystems <- storage
sc <- spark_connect(master = "yarn-client", config=config)

#### Read in the CSV data
cols = list(
  raw_character_text = "character",
  spoken_words = "character"
)

spark_read_csv(
  sc,
  name = "simpsons_spark_table",
  path = paste(storage,"/datalake/data/sentiment/simpsons_dataset.csv",sep=""),
  infer_schema = FALSE,
  columns = cols,
  header = TRUE
)

spark_read_csv(
  sc,
  name = "afinn_table",
  path = paste(storage,"/datalake/data/sentiment/AFINN-en-165.txt",sep=""),
  infer_schema = TRUE,
  delimiter = ",",
  header = FALSE
)

#### Create local references for the Spark tables.
afinn_table <- tbl(sc, "afinn_table")
afinn_table <- afinn_table %>% rename(word = V1, value = V2)

simpsons_spark_table <- tbl(sc, "simpsons_spark_table")
  
simpsons_spark_table %>% count()

#### Basic data cleaning
simpsons_spark_table <- 
  simpsons_spark_table %>% 
  rename(raw_char = raw_character_text)

simpsons_spark_table <- 
  simpsons_spark_table %>% 
  na.omit()

#### Show the changes
simpsons_spark_table %>% group_by(raw_char) %>% count() %>% arrange(desc(n))

## Text Mining

#### Remove punctuation
simpsons_spark_table <- 
  simpsons_spark_table %>% 
  mutate(spoken_words = regexp_replace(spoken_words, "[_\"\'():;,.!?\\-]", " ")) 

#### Tokenize
simpsons_spark_table <- 
  simpsons_spark_table %>% 
  ft_tokenizer(input_col="spoken_words",output_col= "word_list")

#### Remove stop words
simpsons_spark_table <- 
  simpsons_spark_table %>% 
  ft_stop_words_remover(input_col = "word_list", output_col = "wo_stop_words")

#### Explode that word lists into a single column of word by characters
simpsons_final_words <- simpsons_spark_table %>%  mutate(word = explode(wo_stop_words)) %>%
  select(word, raw_char) %>%
  filter(nchar(word) > 2) %>%
  compute("simpsons_spark_table")

#### Find the top 30 characters by number of words.
top_chars <- simpsons_final_words %>% 
  group_by(raw_char) %>% 
  count() %>% 
  arrange(desc(n)) %>% 
  head(30) %>% 
  as.data.frame()

top_chars <- as.vector(top_chars$raw_char)

## Sentiment Analysis
# This is simply taking the AFINN word list and joining it with the words by character
# list. The AFINN value is summed and then weighted according to number of words said

happiest_characters <- simpsons_final_words %>% 
  filter(raw_char %in% top_chars) %>%
  inner_join(afinn_table) %>% 
  group_by(raw_char) %>% 
  summarise(weighted_sum = sum(value)/count()) %>%
  arrange(desc(weighted_sum)) %>% 
  as.data.frame()

happiest_characters

p <-
  ggplot(happiest_characters, aes(reorder(raw_char,weighted_sum), weighted_sum))+
  geom_col(width = 0.7) + 
  coord_flip()
p

## Linear Model

sentences <- simpsons_spark_table %>%  
  mutate(word = explode(wo_stop_words)) %>% 
  select(spoken_words, word) %>%  
  filter(nchar(word) > 2) %>% 
  compute("simpsons_spark_table")

sentence_values <- sentences %>% 
  inner_join(afinn_table) %>% 
  group_by(spoken_words) %>% 
  summarise(weighted_sum = sum(value)/count())

sentence_values_tokenized <- 
  sentence_values %>% 
  mutate(spoken_words = regexp_replace(spoken_words, "[_\"\'():;,.!?\\-]", " ")) %>%
  ft_tokenizer(input_col="spoken_words",output_col= "word_list") %>%
  ft_stop_words_remover(input_col = "word_list", output_col = "wo_stop_words")


w2v <- ft_word2vec(sentence_values_tokenized, input_col = "wo_stop_words", output_col = "result", min_count = 2)

lm_model <- w2v %>% ml_linear_regression(weighted_sum ~ result)

ml_save(
   lm_model,
   paste(Sys.getenv("STORAGE"),"/datalake/data/sentiment/lm_model_r",sep=""),
   overwrite = TRUE
)

sentiment_pipeline <- ml_pipeline(sc) %>%
  ft_tokenizer(input_col="spoken_words",output_col= "word_list") %>%
  ft_stop_words_remover(input_col = "word_list", output_col = "wo_stop_words") %>%
  ft_word2vec(input_col = "wo_stop_words", output_col = "result", min_count = 10) %>%
  ft_r_formula(weighted_sum ~ result) %>% 
  ml_linear_regression()

sentiment_model <- ml_fit(sentiment_pipeline,sentence_values)

ml_regression_evaluator(ml_transform(sentiment_model,sentence_values), label_col = "label",
  prediction_col = "prediction", metric_name = "rmse")

ml_save(
   sentiment_model,
   paste(Sys.getenv("STORAGE"),"/datalake/data/sentiment/sentiment_model_r",sep=""),
   overwrite = TRUE
)