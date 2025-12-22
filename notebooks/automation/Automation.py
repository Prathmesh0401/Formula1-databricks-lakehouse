# Databricks notebook source
# MAGIC %run ../_Includes/01_adls_oauth_config
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS calculated_race_results
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://presentation@stformula1dlake.dfs.core.windows.net/gold/calculated_race_results';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL delta.`abfss://presentation@stformula1dlake.dfs.core.windows.net/gold/calculated_race_results`;
# MAGIC

# COMMAND ----------

spark.read.format("delta") \
  .load("abfss://presentation@stformula1dlake.dfs.core.windows.net/gold/calculated_race_results") \
  .limit(10) \
  .display()


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS constructor_standing
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://presentation@stformula1dlake.dfs.core.windows.net/gold/constructor_standing';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL delta.`abfss://presentation@stformula1dlake.dfs.core.windows.net/gold/constructor_standing`;
# MAGIC

# COMMAND ----------

spark.read.format("delta") \
  .load("abfss://presentation@stformula1dlake.dfs.core.windows.net/gold/constructor_standing") \
  .limit(10) \
  .display()


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS driver_standing
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://presentation@stformula1dlake.dfs.core.windows.net/gold/driver_standing';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL delta.`abfss://presentation@stformula1dlake.dfs.core.windows.net/gold/driver_standing`;
# MAGIC

# COMMAND ----------

spark.read.format("delta") \
  .load("abfss://presentation@stformula1dlake.dfs.core.windows.net/gold/driver_standing") \
  .limit(10) \
  .display()


# COMMAND ----------

dbutils.notebook.exit("success")