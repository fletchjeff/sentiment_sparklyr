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

sentence_scores <- sentence_values %>% 
  mutate(sent_score = ifelse(weighted_sum > weighted_sum_mean,1,0))

sentence_values_tokenized <- 
  sentence_scores %>% 
  ft_tokenizer(input_col="spoken_words",output_col= "word_list") %>%
  ft_stop_words_remover(input_col = "word_list", output_col = "wo_stop_words")

#w2v_model <- ft_word2vec(sc, 
#                         input_col = "wo_stop_words", 
#                         output_col = "result", 
#                         min_count = 5,
#                         max_iter = 25,
#                         vector_size = 400,
#                         step_size = 0.0125
#                        )

#w2v_model_fitted <- ml_fit(w2v_model,sentence_values_tokenized)

#ml_save(
#   w2v_model_fitted,
#   paste(Sys.getenv("STORAGE"),"/datalake/data/sentiment/w2v_model_fitted",sep=""),
#   overwrite = TRUE
#)

w2v_model_fitted <- ml_load(
  sc, 
  paste(Sys.getenv("STORAGE"),"/datalake/data/sentiment/w2v_model_fitted",sep="")
)


#ml_find_synonyms(w2v_model_fitted,"doh",2)

w2v_transformed <- ml_transform(w2v_model_fitted, sentence_values_tokenized)

#w2v_transformed <- w2v_transformed %>% mutate(sent_score = ifelse(weighted_sum>mean,1,0))


w2v_transformed_split <- w2v_transformed %>% sdf_random_split(training=0.7, test = 0.3)

lr_model <- w2v_transformed_split$training %>% select(result,sent_score) %>% 
  ml_logistic_regression(
    sent_score ~ result,
    max_iter=500, 
    elastic_net_param=0.0,
    reg_param = 0.01
  )

ml_save(
   lr_model,
   paste(Sys.getenv("STORAGE"),"/datalake/data/sentiment/lr_model",sep=""),
   overwrite = TRUE
)

lr_model <- ml_load(
  sc, 
  paste(Sys.getenv("STORAGE"),"/datalake/data/sentiment/lr_model",sep="")
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

#sentiment_pipeline <- ml_pipeline(sc) %>%
#  ft_tokenizer(input_col="spoken_words",output_col= "word_list") %>%
#  ft_stop_words_remover(input_col = "word_list", output_col = "wo_stop_words") %>%
#  ft_word2vec(input_col = "wo_stop_words", output_col = "result", min_count = 10) %>%
#  ft_r_formula(sent_score ~ result) %>% 
#  ml_linear_regression()
#
#sentiment_model <- ml_fit(sentiment_pipeline,sentence_values)
#
#ml_save(
#   sentiment_model,
#   paste(Sys.getenv("STORAGE"),"/datalake/data/sentiment/sentiment_model_r",sep=""),
#   overwrite = TRUE
#)