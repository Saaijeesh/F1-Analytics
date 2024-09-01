# Databricks notebook source
# MAGIC %md
# MAGIC # Access azure data lake storage gen2 using cluster scoped credentials
# MAGIC 1. Set spark config
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuit csv file

# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@formula1storageaccdl.dfs.core.windows.net'))

# COMMAND ----------

display(spark.read.csv('abfss://demo@formula1storageaccdl.dfs.core.windows.net/circuits.csv'))

# COMMAND ----------

