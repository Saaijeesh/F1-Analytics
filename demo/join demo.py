# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits")
display(circuits_df)

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races")
display(races_df)

# COMMAND ----------

cir_race_df=circuits_df.join(races_df,on="circuit_id",how="left")
display(cir_race_df)

# COMMAND ----------

