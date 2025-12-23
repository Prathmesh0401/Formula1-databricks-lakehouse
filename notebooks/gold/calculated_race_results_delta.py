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

from pyspark.sql import functions as F

deduped_df = (
    calculated_race_results_df
    .dropDuplicates(["race_year", "race_name"])
)

(
    delta_table.alias("t")
    .merge(
        deduped_df.alias("s"),
        "t.race_year = s.race_year AND t.race_name = s.race_name"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

spark.read.format('delta').load(gold_path).display()


# COMMAND ----------

dbutils.notebook.exit("success")