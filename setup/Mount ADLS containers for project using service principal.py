# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure DataLake containers for the project 
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name,container_name):

    # Get secrets from key vault
    client_id=formula1dl_acc_key =dbutils.secrets.get(scope='formula1-scope', key='formula1-clientid')
    tenant_id=formula1dl_acc_key =dbutils.secrets.get(scope='formula1-scope', key='formula1-tenantid')
    client_secret=formula1dl_acc_key =dbutils.secrets.get(scope='formula1-scope', key='formula1-client-secret')

    # Get spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    # Mount storage account containers
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC # Mount Raw container
# MAGIC

# COMMAND ----------

mount_adls('formula1storageaccdl','raw')

# COMMAND ----------

mount_adls('formula1storageaccdl','demo')

# COMMAND ----------

mount_adls('formula1storageaccdl','presentation')

# COMMAND ----------

mount_adls('formula1storageaccdl','processed')

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1storageaccdl/demo")

# COMMAND ----------

