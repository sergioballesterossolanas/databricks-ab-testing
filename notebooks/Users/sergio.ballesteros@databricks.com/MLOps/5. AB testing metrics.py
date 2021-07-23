# Databricks notebook source
# MAGIC %md
# MAGIC ## Computing metrics
# MAGIC Great, now we have a table were we store the predictions and a table where we have the ground truth of the users who received predictions (we can assume that there is such a feedback loop).
# MAGIC 
# MAGIC <img src="https://github.com/sergioballesterossolanas/databricks-ab-testing/blob/master/img/arch_4.png?raw=true" width="1000"/>
# MAGIC 
# MAGIC 
# MAGIC In this notebook we are going to compare the predictions with the actual responses for the models A and B over time. We will compute the Precision Recall AUC in 1 minute buckets.
# MAGIC 
# MAGIC We will save these results in a Delta table so that we can read it from Databricks SQL. This will allow us to track the quality of both models over time and set up alerts when the quality of the models decrease over a certain threshold. This could be an input to retrain the models with fresher data. This process could be manual, but also could be easily automated by creating a job.
# MAGIC 
# MAGIC 
# MAGIC <img src="https://miro.medium.com/max/3200/1*dCy-F02P3u4kWruKbz4FuA.png" width="1000"/>

# COMMAND ----------

# MAGIC %md
# MAGIC # Import libraries

# COMMAND ----------

from pyspark.ml.functions import vector_to_array
import pyspark.sql.functions as F
from sklearn.metrics import precision_recall_curve
from sklearn.metrics import auc
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC # Helper function

# COMMAND ----------

@pandas_udf("double", PandasUDFType.GROUPED_AGG)
def compute_metric(gt, p):
  precision, recall, thresholds = precision_recall_curve(gt, p)
  return auc(recall, precision)

# COMMAND ----------

# MAGIC %md
# MAGIC # Compute the Precision - Recall AUC metric

# COMMAND ----------

df_pred = (
  spark
  .read
  .table("risk_stream_predictions")
  .select("group", "id", "prediction", vector_to_array(F.col("probability")).getItem(1).alias("prob"), "timestamp")
)

df_gt = (
  spark
  .read
  .table("default.german_credit_data")
  .select("id", "risk")
  .withColumn("ground_truth", F.when(F.col("risk")=="good", 0).otherwise(1))
)

df_metrics = (
  df_gt
  .join(df_pred, on="id", how="inner")
  .withColumn("bucket", F.floor((F.col("timestamp") - df_pred.select(F.min("timestamp")).collect()[0][0])/60))
  .groupby("group", "bucket")
  .agg(compute_metric("ground_truth", "prediction").alias("pr_auc"))
  .na
  .drop()
)
display(df_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save metrics to a Delta Table

# COMMAND ----------

df_metrics.write.mode("overwrite").format("delta").saveAsTable("risk_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC #See the dashboard on Databricks SQL https://e2-demo-west.cloud.databricks.com/sql/dashboards/02566bf1-3ecd-4d63-b3ba-b6ccf859a530-risk-demo

# COMMAND ----------
