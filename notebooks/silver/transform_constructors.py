# Databricks notebook source
# MAGIC %run ../_Includes/01_adls_oauth_config

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

bronze_path = "abfss://processed@stformula1dlake.dfs.core.windows.net/bronze/constructors"

constructors_df = spark.read.parquet(bronze_path)


# COMMAND ----------

constructors_silver_df = (
    constructors_df
    .select(
        col("constructor_id"),
        col("name").alias("constructor_name"),
        col("nationality")
    )
    .dropDuplicates(["constructor_id"])
)


# COMMAND ----------

silver_path = "abfss://processed@stformula1dlake.dfs.core.windows.net/silver/constructors"

(
    constructors_silver_df
    .write
    .mode("overwrite")
    .parquet(silver_path)
)


# COMMAND ----------

spark.read.parquet(silver_path).display()


# COMMAND ----------

dbutils.notebook.exit("success")