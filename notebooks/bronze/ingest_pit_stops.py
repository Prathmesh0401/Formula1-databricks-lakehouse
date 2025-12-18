# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, ArrayType
from pyspark.sql.functions import col, current_timestamp, explode


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

raw_path = "abfss://raw@stformula1dlake.dfs.core.windows.net/pit_stops.json"
bronze_path = "abfss://processed@stformula1dlake.dfs.core.windows.net/bronze/pit_stops"


# COMMAND ----------

pit_stops_schema = StructType([
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField(
        "pitStops",
        ArrayType(
            StructType([
                StructField("stop", IntegerType(), True),
                StructField("lap", IntegerType(), True),
                StructField("time", StringType(), True),
                StructField("duration", StringType(), True),
                StructField("milliseconds", IntegerType(), True)
            ])
        ),
        True
    )
])


# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).json(raw_path)
display(pit_stops_df)

# COMMAND ----------

pit_stops_exploded_df = (
    pit_stops_df
    .select(
        col("raceId").alias("race_id"),
        col("driverId").alias("driver_id"),
        explode(col("pitStops")).alias("pit_stop")
    )
)

pit_stops_bronze_df = (
    pit_stops_exploded_df
    .select(
        col("race_id"),
        col("driver_id"),
        col("pit_stop.stop").alias("stop_number"),
        col("pit_stop.lap").alias("lap"),
        col("pit_stop.time").alias("time"),
        col("pit_stop.duration").alias("duration"),
        col("pit_stop.milliseconds").alias("milliseconds")
    )
    .withColumn("ingestion_date", current_timestamp())
)


# COMMAND ----------

(
    pit_stops_bronze_df
    .write
    .mode("overwrite")
    .parquet(bronze_path)
)


# COMMAND ----------

spark.read.parquet(bronze_path).display()


# COMMAND ----------

dbutils.notebook.exit("SUCCESS")