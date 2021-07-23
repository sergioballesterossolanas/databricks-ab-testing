# Databricks notebook source
# MAGIC %md
# MAGIC # Download the dataset
# MAGIC <img src="https://thumbs.dreamstime.com/b/credit-risk-message-bubble-word-cloud-collage-business-concept-background-credit-risk-message-bubble-word-cloud-collage-business-216251701.jpg" width="400"/>
# MAGIC 
# MAGIC This notebook will download the German Credit Risk dataset:
# MAGIC 
# MAGIC https://www.kaggle.com/uciml/german-credit
# MAGIC 
# MAGIC It will save the data in the *german_credit_data* Delta table

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://raw.githubusercontent.com/sergioballesterossolanas/databricks-ab-testing/master/german_credit_data.csv -O /dbfs/tmp/german_credit_data.csv

# COMMAND ----------

permanent_table_name = "german_credit_data"

df = (
  spark
  .read
  .option("inferSchema", "true") 
  .option("header", "true") 
  .option("sep", ",") 
  .csv("/tmp/german_credit_data.csv")
)

df.write.format("delta").mode("overwrite").saveAsTable(permanent_table_name)