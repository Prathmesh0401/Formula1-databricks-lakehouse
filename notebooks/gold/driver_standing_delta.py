# Databricks notebook source
# MAGIC %run ../_Includes/01_adls_oauth_config
# MAGIC

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, desc, sum, countDistinct

# COMMAND ----------


calc_df = spark.read.format("delta").load(
    "abfss://presentation@stformula1dlake.dfs.core.windows.net/gold/calculated_race_results"
)


# COMMAND ----------

driver_agg_df = (
    calc_df
    .groupBy("race_year", "driver_id", "driver_name")
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

driver_standing_df = driver_agg_df.withColumn(
    "rank", dense_rank().over(rank_window)
)


# COMMAND ----------

from delta.tables import DeltaTable

gold_path = "abfss://presentation@stformula1dlake.dfs.core.windows.net/gold/driver_standing"

delta_table = DeltaTable.forPath(spark, gold_path)

(
    delta_table.alias("t")
    .merge(
        driver_standing_df.alias("s"),
        "t.race_year = s.race_year AND t.driver_name = s.driver_name"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

spark.read.format("delta").load(gold_path).display()

# COMMAND ----------

dbutils.notebook.exit("success")