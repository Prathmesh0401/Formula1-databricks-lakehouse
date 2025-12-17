# Databricks notebook source
spark

# COMMAND ----------

spark.version

# COMMAND ----------

spark.conf.getAll

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_date()
# MAGIC

# COMMAND ----------

dbutils.fs.ls("/")


# COMMAND ----------

dbutils.fs.help()


# COMMAND ----------

for file in dbutils.fs.ls("/databricks-datasets/COVID/"):
  if file.name.endswith("/"):
    print(file.path)

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# MAGIC %md
# MAGIC #Notebook Introduction
# MAGIC ##Code
# MAGIC ###Magic Commands
# MAGIC - %Python
# MAGIC - %SQL
# MAGIC - %Scala
# MAGIC - %md
# MAGIC - %fs
# MAGIC - %sh
# MAGIC - %run

# COMMAND ----------

# MAGIC %sh
# MAGIC ps -ef
# MAGIC

# COMMAND ----------

dbutils.fs.put(
    "/tmp/f1_test.txt",
    "Databricks fundamentals validated",
    overwrite=True
)


# COMMAND ----------

dbutils.fs.head("/tmp/f1_test.txt")


# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.get("env")


# COMMAND ----------

dbutils.notebook.exit("Databricks fundamentals completed successfully")
