# Databricks notebook source
# MAGIC %md
# MAGIC # Access azure data lake storage gen2 using shared access storage
# MAGIC 1. Set spark config
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuit csv file

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

formula1dl_sas_key=dbutils.secrets.get(scope='formula1-scope',key='formula1-sas-access')


# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1storageaccdl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1storageaccdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1storageaccdl.dfs.core.windows.net", formula1dl_sas_key)


# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@formula1storageaccdl.dfs.core.windows.net'))

# COMMAND ----------

display(spark.read.csv('abfss://demo@formula1storageaccdl.dfs.core.windows.net/circuits.csv'))

# COMMAND ----------

