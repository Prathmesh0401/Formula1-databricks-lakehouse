# Databricks notebook source
# MAGIC %run ../_Includes/01_adls_oauth_config

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

bronze_path = "abfss://processed@stformula1dlake.dfs.core.windows.net/bronze/results"

results_df = spark.read.parquet(bronze_path)


# COMMAND ----------


results_silver_df = (
    results_df
    .select(
        col("result_id"),
        col("race_id"),
        col("driver_id"),
        col("constructor_id"),
        col("grid"),
        col("position"),
        col("points"),
        col("laps"),
        col("milliseconds"),
        col("fastestLap").alias("fastest_lap"),
        col("rank"),
        col("fastestLapTime").alias("fastest_lap_time"),
        col("fastestLapSpeed").alias("fastest_lap_speed"),
        col("status_id")
    )
    .dropDuplicates(["result_id"])
)


# COMMAND ----------

silver_path = "abfss://processed@stformula1dlake.dfs.core.windows.net/silver/results"

(
    results_silver_df
    .write
    .mode("overwrite")
    .parquet(silver_path)
)


# COMMAND ----------

spark.read.parquet(silver_path).display()


# COMMAND ----------

dbutils.notebook.exit("success")