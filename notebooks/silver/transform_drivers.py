# Databricks notebook source
# MAGIC %run ../_Includes/01_adls_oauth_config

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws


# COMMAND ----------

bronze_path = "abfss://processed@stformula1dlake.dfs.core.windows.net/bronze/drivers"

drivers_df = spark.read.parquet(bronze_path)


# COMMAND ----------


drivers_silver_df = (
    drivers_df
    .select(
        col("driver_id"),
        concat_ws(" ", col("forename"), col("surname")).alias("driver_name"),
        col("dob"),
        col("nationality")
    )
    .dropDuplicates(["driver_id"])
)


# COMMAND ----------

silver_path = "abfss://processed@stformula1dlake.dfs.core.windows.net/silver/drivers"

(
    drivers_silver_df
    .write
    .mode("overwrite")
    .parquet(silver_path)
)


# COMMAND ----------

spark.read.parquet(silver_path).display()


# COMMAND ----------

dbutils.notebook.exit("success")