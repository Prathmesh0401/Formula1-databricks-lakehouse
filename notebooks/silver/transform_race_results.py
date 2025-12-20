# Databricks notebook source
# MAGIC %run ../_Includes/01_adls_oauth_config

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

results_df = spark.read.parquet(
    "abfss://processed@stformula1dlake.dfs.core.windows.net/silver/results"
)

races_df = spark.read.parquet(
    "abfss://processed@stformula1dlake.dfs.core.windows.net/silver/races"
)

drivers_df = spark.read.parquet(
    "abfss://processed@stformula1dlake.dfs.core.windows.net/silver/drivers"
)

constructors_df = spark.read.parquet(
    "abfss://processed@stformula1dlake.dfs.core.windows.net/silver/constructors"
)

circuits_df = spark.read.parquet(
    "abfss://processed@stformula1dlake.dfs.core.windows.net/silver/circuits"
)


# COMMAND ----------


race_results_df = (
    results_df
    .join(drivers_df, "driver_id", "inner")
    .join(constructors_df, "constructor_id", "inner")
    .join(races_df, "race_id", "inner")
    .join(circuits_df, "circuit_id", "inner")
    .select(
        col("race_year"),
        col("race_name"),
        col("race_date"),
        col("circuit_id"),
        col("name").alias("circuit_name"),
        col("location"),
        col("country"),
        col("driver_id"),
        col("driver_name"),
        col("constructor_id"),
        col("constructor_name"),
        col("grid"),
        col("position"),
        col("points"),
        col("laps"),
        col("fastest_lap"),
        col("fastest_lap_time"),
        col("fastest_lap_speed")
    )
)


# COMMAND ----------

silver_path = "abfss://processed@stformula1dlake.dfs.core.windows.net/silver/race_results"

(
    race_results_df
    .write
    .mode("overwrite")
    .parquet(silver_path)
)


# COMMAND ----------

spark.read.parquet(silver_path).display()


# COMMAND ----------

dbutils.notebook.exit("success")