# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races")
display(races_df)

# COMMAND ----------

races_filtered=races_df.filter((races_df['race_year']==2019) & (races_df['round']<=5))
display(races_filtered)




# COMMAND ----------

