# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, to_date, current_timestamp


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

raw_path = "abfss://raw@stformula1dlake.dfs.core.windows.net/races.csv"
bronze_path = "abfss://processed@stformula1dlake.dfs.core.windows.net/bronze/races"


# COMMAND ----------

races_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])


# COMMAND ----------

races_df = spark.read.option('header', True).format("csv").schema(races_schema).load(raw_path)

# COMMAND ----------

races_bronze_df = (
    races_df
    .select(
        col("raceId").alias("race_id"),
        col("year").alias("race_year"),
        col("round"),
        col("circuitId").alias("circuit_id"),
        col("name"),
        to_date(col("date"), "yyyy-MM-dd").alias("race_date"),
        col("time"),
        col("url")
    )
    .withColumn("ingestion_date", current_timestamp())
)


# COMMAND ----------

(
    races_bronze_df
    .write
    .mode("overwrite")
    .partitionBy("race_year")
    .parquet(bronze_path)
)


# COMMAND ----------

spark.read.parquet(bronze_path).display()


# COMMAND ----------

dbutils.notebook.exit("success")