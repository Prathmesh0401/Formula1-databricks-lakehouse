# Databricks notebook source
# MAGIC %run ../_Includes/01_adls_oauth_config
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, when

race_results_df = spark.read.parquet(
    "abfss://processed@stformula1dlake.dfs.core.windows.net/silver/race_results"
)


# COMMAND ----------

calculated_race_results_df = (
    race_results_df
    .withColumn("is_win", when(col("position") == 1, 1).otherwise(0))
    .withColumn("is_podium", when(col("position") <= 3, 1).otherwise(0))
)


# COMMAND ----------

gold_path = "abfss://presentation@stformula1dlake.dfs.core.windows.net/gold/calculated_race_results"

(
    calculated_race_results_df
    .write
    .mode("overwrite")
    .parquet(gold_path)
)


# COMMAND ----------

spark.read.parquet(gold_path).display()


# COMMAND ----------

dbutils.notebook.exit("success")