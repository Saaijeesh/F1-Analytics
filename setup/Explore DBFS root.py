# Databricks notebook source
# MAGIC %md
# MAGIC # Explore DBFS root
# MAGIC 1. List all the folders in DBFS root
# MAGIC 2. Interact with DBFS file browser
# MAGIC 3. Upload file with DBFS root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables'))

# COMMAND ----------

display(spark.read.csv('dbfs:/FileStore/tables/circuits.csv'))

# COMMAND ----------

