# Databricks notebook source
# MAGIC %md
# MAGIC # Access azure data lake storage gen2 using service principal
# MAGIC 1. Register Azure AD Application/ Service Principal
# MAGIC 2. Generate a secret/ password for the application
# MAGIC 3. Set Spark config with App/Client ID, Directory/ Tenant ID and Secret 
# MAGIC 4. Assign Role'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

client_id=formula1dl_acc_key =dbutils.secrets.get(scope='formula1-scope', key='formula1-clientid')
tenant_id=formula1dl_acc_key =dbutils.secrets.get(scope='formula1-scope', key='formula1-tenantid')
client_secret=formula1dl_acc_key =dbutils.secrets.get(scope='formula1-scope', key='formula1-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1storageaccdl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1storageaccdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1storageaccdl.dfs.core.windows.net",client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1storageaccdl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1storageaccdl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@formula1storageaccdl.dfs.core.windows.net'))

# COMMAND ----------

display(spark.read.csv('abfss://demo@formula1storageaccdl.dfs.core.windows.net/circuits.csv'))

# COMMAND ----------

