# Databricks notebook source
# MAGIC %md
# MAGIC # Mount ADLS using service principal
# MAGIC 1. Get client id, tenant id, client secret from key vault
# MAGIC 2. Set Spark config with App/Client ID, Directory/ Tenant ID and Secret 
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount (List all mounts, unmount)

# COMMAND ----------

client_id=formula1dl_acc_key =dbutils.secrets.get(scope='formula1-scope', key='formula1-clientid')
tenant_id=formula1dl_acc_key =dbutils.secrets.get(scope='formula1-scope', key='formula1-tenantid')
client_secret=formula1dl_acc_key =dbutils.secrets.get(scope='formula1-scope', key='formula1-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1storageaccdl.dfs.core.windows.net/",
  mount_point = "/mnt/formula1storageaccdl/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1storageaccdl/demo'))

# COMMAND ----------

display(spark.read.csv('/mnt/formula1storageaccdl/demo/circuits.csv'))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula1storageaccdl/demo')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

