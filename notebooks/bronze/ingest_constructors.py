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

raw_path = "abfss://raw@stformula1dlake.dfs.core.windows.net/constructors.json"
bronze_path = "abfss://processed@stformula1dlake.dfs.core.windows.net/bronze/constructors.json"


# COMMAND ----------

constructors_schema = StructType([
    StructField("constructorId", IntegerType(), False),
    StructField("constructorRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])


# COMMAND ----------

constructors_df = spark.read.json(raw_path, schema=constructors_schema)
display(constructors_df)


# COMMAND ----------

constructors_bronze_df = (
    constructors_df
    .select(
        col("constructorId").alias("constructor_id"),
        col("constructorRef").alias("constructor_ref"),
        col("name"),
        col("nationality"),
        col("url")
    )
    .withColumn("ingestion_date", current_timestamp())
)



# COMMAND ----------


(
    constructors_bronze_df
    .write
    .mode("overwrite")
    .parquet(bronze_path)
)

# COMMAND ----------

spark.read.parquet(bronze_path).display()


# COMMAND ----------

dbutils.notebook.exit("success")