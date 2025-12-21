# Databricks notebook source
# MAGIC %run ../_Includes/01_adls_oauth_config
# MAGIC

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, desc, sum, countDistinct

# COMMAND ----------


calc_df = spark.read.parquet(
    "abfss://presentation@stformula1dlake.dfs.core.windows.net/gold/calculated_race_results"
)


# COMMAND ----------

constructor_agg_df = (
    calc_df
    .groupBy("race_year", "constructor_id", "constructor_name")
    .agg(
        sum("points").alias("total_points"),
        sum("is_win").alias("wins"),
        sum("is_podium").alias("podiums"),
        countDistinct("race_name").alias("races_participated")
    )
)


# COMMAND ----------

rank_window = Window.partitionBy("race_year").orderBy(
    desc("total_points"),
    desc("wins")
)

constructor_standing_df = constructor_agg_df.withColumn(
    "rank", dense_rank().over(rank_window)
)


# COMMAND ----------

gold_path = "abfss://presentation@stformula1dlake.dfs.core.windows.net/gold/constructor_standing"

(
    constructor_standing_df
    .write
    .mode("overwrite")
    .partitionBy("race_year")
    .parquet(gold_path)
)


# COMMAND ----------

spark.read.parquet(gold_path).display()


# COMMAND ----------

dbutils.notebook.exit("success")