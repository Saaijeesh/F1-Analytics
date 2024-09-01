# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")
display(race_results_df)

# COMMAND ----------

race_results_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM v_race_results
# MAGIC WHERE race_year==2020
# MAGIC

# COMMAND ----------

race_result_2020=spark.sql("select count(*) FROM v_race_results WHERE race_year==2020")
display(race_result_2020)

# COMMAND ----------

p_race_year=2019
race_result_2019=spark.sql(f"select count(*) FROM v_race_results WHERE race_year=={p_race_year}")
display(race_result_2019)

# COMMAND ----------

race_results_df.createTempView("v_race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN GLOBAL_TEMP;

# COMMAND ----------

