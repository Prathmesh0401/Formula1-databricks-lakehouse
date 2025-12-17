# Databricks notebook source
storage_account = "stformula1dlake"

client_id = dbutils.secrets.get(scope="formula1-scope", key="sp-client-id")
client_secret = dbutils.secrets.get(scope="formula1-scope", key="sp-client-secret")
tenant_id = dbutils.secrets.get(scope="formula1-scope", key="sp-tenant-id")

# COMMAND ----------

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
    client_id
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
    client_secret
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
)

# COMMAND ----------

display(dbutils.fs.ls(f"abfss://raw@{storage_account}.dfs.core.windows.net/"))
