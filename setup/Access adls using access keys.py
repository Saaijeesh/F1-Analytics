# Databricks notebook source
# MAGIC %md
# MAGIC # Access azure data lake storage gen2 using access key
# MAGIC 1. Set spark config
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuit csv file

# COMMAND ----------

formula1dl_acc_key =dbutils.secrets.get(scope='formula1-scope', key='formula1-dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1storageaccdl.dfs.core.windows.net",
    formula1dl_acc_key
)

# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@formula1storageaccdl.dfs.core.windows.net'))

# COMMAND ----------

display(spark.read.csv('abfss://demo@formula1storageaccdl.dfs.core.windows.net/circuits.csv'))

# COMMAND ----------

