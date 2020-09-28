# Sentiment Analysis Prototype
This project is a Cloudera Machine Learning 
([CML](https://www.cloudera.com/products/machine-learning.html)) **Applied Machine Learning 
Project Prototype**. It has all the code and data needed to deploy an end-to-end machine 
learning project in a running CML instance.

![frink](images/265282.jpg)

Images Source: https://toddwschneider.com/posts/the-simpsons-by-the-data/

## Project Overview
This project builds two different Sentiment Analysis models. One is based on 



interpretability project discussed in more 
detail [this blog post](https://blog.cloudera.com/visual-model-interpretability-for-telco-churn-in-cloudera-data-science-workbench/). 

The initial idea and code comes from the FFL Interpretability report which is now freely 
available and you can read the full report [here](https://ff06-2020.fastforwardlabs.com/)

![table_view](images/table_view.png)

The goal is to build a classifier model using Logistic Regression to predict the churn 
probability for a group of customers from a telecoms company. On top that, the model 
can then be interpreted using [LIME](https://github.com/marcotcr/lime). Both the Logistic 
Regression and LIME models are then deployed using CML's real-time model deployment 
capability and finally a basic flask based web application is deployed that will let 
you interact with the real-time model to see which factors in the data have the most 
influence on the churn probability.

By following the notebooks in this project, you will understand how to perform similar 
classification tasks on CML as well as how to use the platform's major features to your 
advantage. These features include **streamlined model experimentation**, 
**point-and-click model deployment**, and **ML app hosting**.

We will focus our attention on working within CML, using all it has to offer, while
glossing over the details that are simply standard data science.
We trust that you are familiar with typical data science workflows
and do not need detailed explanations of the code.
Notes that are *specific to CML* will be emphasized in **block quotes**.