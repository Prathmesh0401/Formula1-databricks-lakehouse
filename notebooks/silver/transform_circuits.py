# Databricks notebook source
# MAGIC %run ../_Includes/01_adls_oauth_config

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

bronze_path = "abfss://processed@stformula1dlake.dfs.core.windows.net/bronze/circuits"

circuits_df = spark.read.parquet(bronze_path)


# COMMAND ----------


circuits_silver_df = (
    circuits_df
    .select(
        col("circuit_id"),
        col("circuit_ref"),
        col("name"),
        col("location"),
        col("country"),
        col("latitude"),
        col("longitude"),
        col("altitude")
    )
    .dropDuplicates(["circuit_id"])
)


# COMMAND ----------

silver_path = "abfss://processed@stformula1dlake.dfs.core.windows.net/silver/circuits"

(
    circuits_silver_df
    .write
    .mode("overwrite")
    .parquet(silver_path)
)


# COMMAND ----------

spark.read.parquet(silver_path).display()


# COMMAND ----------

dbutils.notebook.exit("success")