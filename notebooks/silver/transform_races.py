# Databricks notebook source
# MAGIC %run ../_Includes/01_adls_oauth_config

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

bronze_path = "abfss://processed@stformula1dlake.dfs.core.windows.net/bronze/races"

races_df = spark.read.parquet(bronze_path)


# COMMAND ----------

races_silver_df = (
    races_df
    .select(
        col("race_id"),
        col("race_year"),
        col("round"),
        col("circuit_id"),
        col("name").alias("race_name"),
        col("race_date")
    )
    .dropDuplicates(["race_id"])
)


# COMMAND ----------

silver_path = "abfss://processed@stformula1dlake.dfs.core.windows.net/silver/races"

(
    races_silver_df
    .write
    .mode("overwrite")
    .parquet(silver_path)
)


# COMMAND ----------

spark.read.parquet(silver_path).display()


# COMMAND ----------

dbutils.notebook.exit("success")