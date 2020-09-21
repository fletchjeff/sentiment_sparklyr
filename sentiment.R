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
# The text data is a very simple dataset. Its 2 columns, one for the character, and on for their
# dialog. Since we know that its 2 columns of `characters` we will set the schema and save
# spark the trouble of doing it automatically.

#```
#Miss Hoover,"No, actually, it was a little of both. Sometimes when a disease is in all he magazines and all the news shows, it's only natural that you think you have it."
#Lisa Simpson,Where's Mr. Bergstrom?
#Miss Hoover,I don't know. Although I'd sure like to talk to him. He didn't touch my lesson plan. What did he teach you?
#Lisa Simpson,That life is worth living.
#```

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



# The other dataset we will use is the AFINN list. https://github.com/fnielsen/afinn/tree/master/afinn/data 
# its 2 column, the first being the word and the second and integer value for its `valance`. 

#```
#abandon	-2
#abandoned	-2
#abandons	-2
#abducted	-2
#```

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

as.data.frame(head(simpsons_spark_table))
  
simpsons_spark_table %>% count()

#### Basic data cleaning

# renaming a column
simpsons_spark_table <- 
  simpsons_spark_table %>% 
  rename(raw_char = raw_character_text)

# droping null / NA values
simpsons_spark_table <- 
  simpsons_spark_table %>% 
  na.omit()

#### Show the changes
simpsons_spark_table %>% group_by(raw_char) %>% count() %>% arrange(desc(n))

## Text Mining
# https://spark.rstudio.com/guides/textmining/


#### Remove punctuation
# * mutate can use python.
simpsons_spark_table <- 
  simpsons_spark_table %>% 
  mutate(spoken_words = regexp_replace(spoken_words, "\'", "")) %>%
  mutate(spoken_words = regexp_replace(spoken_words, "[_\"():;,.!?\\-]", " "))

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

## Creating a Linear Model using word2vec

sentences <- simpsons_spark_table %>%  
  mutate(word = explode(wo_stop_words)) %>% 
  select(spoken_words, word) %>%  
  filter(nchar(word) > 2) %>% 
  compute("simpsons_spark_table")

#sentence_values <- sentences %>% 
#  inner_join(afinn_table) %>% 
#  group_by(spoken_words) %>% 
#  summarise(weighted_sum = sum(value)/count())

sentence_values <- sentences %>% 
  inner_join(afinn_table) %>% 
  group_by(spoken_words) %>% 
  summarise(weighted_sum = sum(value))

weighted_sum_summary <- sentence_values %>% sdf_describe(cols="weighted_sum")

weighted_sum_summary

weighted_sum_mean <- as.data.frame(weighted_sum_summary)[2,2]

sentence_values %>% 
  mutate(sent_score = ifelse(weighted_sum > weighted_sum_mean,1,0))

sentence_values_tokenized <- 
  sentence_scores %>% 
  ft_tokenizer(input_col="spoken_words",output_col= "word_list") %>%
  ft_stop_words_remover(input_col = "word_list", output_col = "wo_stop_words")

w2v_model <- ft_word2vec(sc, 
                         input_col = "wo_stop_words", 
                         output_col = "result", 
                         #min_count = 5,
                         #max_iter = 25,
                         #vector_size = 400,
                         #step_size = 0.0125
                        )

w2v_model_fitted <- ml_fit(w2v_model,sentence_values_tokenized)

w2v_model_fitted <- ml_load(sc,"s3a://demo-aws-1/datalake/data/sentiment/w2v_model_fitted")


#ml_find_synonyms(w2v_model_fitted,"doh",2)

w2v_transformed <- ml_transform(w2v_model_fitted, sentence_values_tokenized)

w2v_transformed <- w2v_transformed %>% mutate(sent_score = ifelse(weighted_sum>mean,1,0))


w2v_transformed_split <- w2v_transformed %>% sdf_random_split(training=0.7, test = 0.3)

maxIter= 500
elasticNetParam = 0.0
regParam = 0.01

lr_model <- w2v_transformed_split$training %>% select(result,sent_score) %>% 
  ml_logistic_regression(
    sent_score ~ result,
    #max_iter=maxIter, 
    #elastic_net_param=elasticNetParam,
    #reg_param = regParam
  )


density_plot <- function(X) {
  hist(X, prob=TRUE, col="grey", breaks=500, xlim=c(-10,10), ylim=c(0,0.2))# prob=TRUE for probabilities not counts
  lines(density(X), col="blue", lwd=2) # add a density estimate with defaults
  #lines(density(X, adjust=2), lty="dotted", col="darkgreen", lwd=2) 
}

density_plot(as.data.frame(w2v_transformed %>% select(weighted_sum))$weighted_sum)


pred_lr_training <- ml_predict(lr_model, w2v_transformed_split$training)

pred_lr_test<- ml_predict(lr_model, w2v_transformed_split$test)

ml_binary_classification_evaluator(pred_lr_training,label_col = "sent_score",
                        prediction_col = "prediction", metric_name = "areaUnderROC")

ml_binary_classification_evaluator(pred_lr_test,label_col = "sent_score",
                        prediction_col = "prediction", metric_name = "areaUnderROC")

## Creating a reusable model pipeline.

sentiment_pipeline <- ml_pipeline(sc) %>%
  ft_tokenizer(input_col="spoken_words",output_col= "word_list") %>%
  ft_stop_words_remover(input_col = "word_list", output_col = "wo_stop_words") %>%
  ft_word2vec(input_col = "wo_stop_words", output_col = "result", min_count = 10) %>%
  ft_r_formula(sent_score ~ result) %>% 
  ml_linear_regression()

sentiment_model <- ml_fit(sentiment_pipeline,sentence_values)

ml_save(
   sentiment_model,
   paste(Sys.getenv("STORAGE"),"/datalake/data/sentiment/sentiment_model_r",sep=""),
   overwrite = TRUE
)