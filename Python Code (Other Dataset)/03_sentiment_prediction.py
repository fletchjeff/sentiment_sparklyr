import numpy as np
import pickle
import tensorflow as tf
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences


embedding_dim = 100
max_length = 16
trunc_type='post'
padding_type='post'

def predict_sentiment(args):
  sent = args["sent"]
  sent = np.array([sent])
  
  #Load the previously built Tokenizer
  with open('pyFunc_tests/models/test_tokenizer.pickle', 'rb') as handle:
    loaded_tokenizer = pickle.load(handle)

  #Pad sequences
  sequences = loaded_tokenizer.texts_to_sequences(sent)
  padded = pad_sequences(sequences, maxlen=max_length, padding=padding_type, truncating=trunc_type)
  test_example = padded
  
  #Load Model
  model = tf.keras.models.load_model("pyFunc_tests/models/test_model.h5")
  pred_conf = model.predict(test_example)
  pred_class = (model.predict(test_example) > 0.5).astype("int32")
  
  #print("%s sentiment; %f%% confidence" % (pred_class[0][0], pred_conf[0][np.argmax(pred_conf)] * 100))
  if pred_class[0][0]==0:
    sentiment = 'Negative'
  else:
    sentiment = 'Positive'
  conf = pred_conf[0][np.argmax(pred_conf)] * 100
  percentage_conf = round(conf, 2)
  #return {"sent": sentiment, "confidence": str(pred_conf[0][np.argmax(pred_conf)] * 100)+' %'}
  return {"sentiment": sentiment, "confidence": str(percentage_conf)+' %'}
  

### To make a sample prediction, call the above function, and pass the Input sentence as argument
### Example format:
### predict_sentiment(args = {"sent":"This is a great idea"})