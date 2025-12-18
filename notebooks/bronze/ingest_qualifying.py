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

raw_path = "abfss://raw@stformula1dlake.dfs.core.windows.net/qualifying*.json"
bronze_path = "abfss://processed@stformula1dlake.dfs.core.windows.net/bronze/qualifying"


# COMMAND ----------

qualifying_schema = StructType([
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])


# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).json(raw_path)
display(qualifying_df)

# COMMAND ----------

qualifying_bronze_df = (
    qualifying_df
    .select(
        col("qualifyId").alias("qualify_id"),
        col("raceId").alias("race_id"),
        col("driverId").alias("driver_id"),
        col("constructorId").alias("constructor_id"),
        col("number"),
        col("position"),
        col("q1"),
        col("q2"),
        col("q3")
    )
    .withColumn("ingestion_date", current_timestamp())
)



# COMMAND ----------


(
    qualifying_bronze_df
    .write
    .mode("overwrite")
    .parquet(bronze_path)
)

# COMMAND ----------

spark.read.parquet(bronze_path).display()


# COMMAND ----------

dbutils.notebook.exit("success")