# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, current_timestamp


# COMMAND ----------

circuits_schema = StructType([
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])


# COMMAND ----------

storage_account = "stformula1dlake"

#Override implicit key-based auth
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    ""
)

# OAuth configs
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

raw_path = "abfss://raw@stformula1dlake.dfs.core.windows.net/circuits.csv"

circuits_df = spark.read.option('header', True).format("csv").schema(circuits_schema).load(raw_path)

# COMMAND ----------

circuits_selected_df = (
    circuits_df
    .select(
        col("circuitId").alias("circuit_id"),
        col("circuitRef").alias("circuit_ref"),
        col("name"),
        col("location"),
        col("country"),
        col("lat").alias("latitude"),
        col("lng").alias("longitude"),
        col("alt").alias("altitude"),
        col("url")
    )
    .withColumn("ingestion_date", current_timestamp())
)


# COMMAND ----------

bronze_path = "abfss://processed@stformula1dlake.dfs.core.windows.net/bronze/circuits.csv"

(
    circuits_selected_df
    .write
    .mode("overwrite")
    .parquet(bronze_path)
)


# COMMAND ----------

spark.read.parquet(bronze_path).display()

# COMMAND ----------

dbutils.notebook.exit("Success")