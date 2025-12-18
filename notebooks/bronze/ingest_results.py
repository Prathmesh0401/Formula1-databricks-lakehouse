# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
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

raw_path = "abfss://raw@stformula1dlake.dfs.core.windows.net/results.json"
bronze_path = "abfss://processed@stformula1dlake.dfs.core.windows.net/bronze/results"


# COMMAND ----------

results_schema = StructType([
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", IntegerType(), True)
])


# COMMAND ----------

results_df = spark.read.schema(results_schema).json(raw_path)
display(results_df)

# COMMAND ----------

results_bronze_df = (
    results_df
    .select(
        col("resultId").alias("result_id"),
        col("raceId").alias("race_id"),
        col("driverId").alias("driver_id"),
        col("constructorId").alias("constructor_id"),
        col("number"),
        col("grid"),
        col("position"),
        col("positionText"),
        col("positionOrder"),
        col("points"),
        col("laps"),
        col("time"),
        col("milliseconds"),
        col("fastestLap"),
        col("rank"),
        col("fastestLapTime"),
        col("fastestLapSpeed"),
        col("statusId").alias("status_id")
    )
    .withColumn("ingestion_date", current_timestamp())
)



# COMMAND ----------


(
    results_bronze_df
    .write
    .mode("overwrite")
    .parquet(bronze_path)
)

# COMMAND ----------

spark.read.parquet(bronze_path).display()

# COMMAND ----------

dbutils.notebook.exit("success")