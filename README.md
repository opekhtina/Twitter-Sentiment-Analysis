Project focusing on BigData tools for Machine Learning. 

Work was mainly done in a DataBricks notebook

Results were stored in AWS S3 and loaded to AWS Athena

Results from Athena were loaded to QuickSIght to produce a dashboard

**Overview**

Predicting tweet sentiments about World Cup 2022 using Databricks, AWS s3, Athena and QuickSight as well as simple ML models. 

Data had been previously collected from Twitter using Kinesis 

- There are 998362 rows of data after null values in date column had been dropped 
- Labels are somewhat balanced, with 'negative' being a minority class at 17.5% of all data
  
  <img width="367" alt="image" src="https://github.com/opekhtina/Twitter-Sentiment-Analysis-using-Spark-and-AWS/assets/133146847/ceee38bb-3483-4a98-99b6-8996ac0a72f5">

- it seems like the more followers users have the more likely it is for the to tweet positive tweets

  <img width="531" alt="image" src="https://github.com/opekhtina/Twitter-Sentiment-Analysis-using-Spark-and-AWS/assets/133146847/39119e1a-e825-4f7f-bf7d-c0a9695fa00c">


**Objective**

Exploring DataBricks Community Edition capabilities to wrangle 1 million rows of data and utilize a variety of tools to encourage understandign of their use cases.


**Models used in this project:**

Logistic Regression
Decision Tree
Naive Bayes

**GridSearch done on:**

Logistic Regression

**Sampled GridSearch done on:**

Random Forest

Pipeline was used for the final model

**Conclusions**
- ML in Spark heavily relies on how many cores are available in the cluster
- Tree-based algorithms are much more computationally exprensive and were not able to predict accurately due to lack of depth
- Both linear models (LR and NB) have performed much better in this specific scenario, producing higher scores




