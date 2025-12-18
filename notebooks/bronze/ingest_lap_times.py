# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, current_timestamp


# COMMAND ----------

storage_account = "stformula1dlake"

# Override implicit access-key auth
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    ""
)

# OAuth configuration
spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get("formula1-scope", "sp-client-id")
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get("formula1-scope", "sp-client-secret")
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{dbutils.secrets.get('formula1-scope','sp-tenant-id')}/oauth2/token"
)


# COMMAND ----------

raw_path = "abfss://raw@stformula1dlake.dfs.core.windows.net/lap_times*.csv"

bronze_path = "abfss://processed@stformula1dlake.dfs.core.windows.net/bronze/lap_times"


# COMMAND ----------

lap_times_schema = StructType([
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])


# COMMAND ----------

lap_times_df = spark.read.option("header", True).schema(lap_times_schema).csv(raw_path)
display(lap_times_df)


# COMMAND ----------

lap_times_bronze_df = (
    lap_times_df
    .select(
        col("raceId").alias("race_id"),
        col("driverId").alias("driver_id"),
        col("lap"),
        col("position"),
        col("time"),
        col("milliseconds")
    )
    .withColumn("ingestion_date", current_timestamp())
)




# COMMAND ----------

(
    lap_times_bronze_df
    .write
    .mode("overwrite")
    .parquet(bronze_path)
)

# COMMAND ----------

spark.read.parquet(bronze_path).display()


# COMMAND ----------

dbutils.notebook.exit("success")