# Databricks notebook source
dbutils.widgets.text('p_file_date',"2021-03-28")
v_file_date= dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common functions"

# COMMAND ----------

drivers_df=spark.read.format('delta').load(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("number","driver_number") \
    .withColumnRenamed("name","driver_name") \
    .withColumnRenamed("nationality","driver_nationality")


# COMMAND ----------

constructors_df=spark.read.format('delta').load(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name","team")

# COMMAND ----------

circuits_df=spark.read.format('delta').load(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location","circuit_location")

# COMMAND ----------

races_df=spark.read.format('delta').load(f"{processed_folder_path}/races") \
    .withColumnRenamed("name","race_name") \
    .withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

results_df=spark.read.format('delta').load(f"{processed_folder_path}/results") \
    .filter(f"file_date == '{v_file_date}'")\
    .withColumnRenamed("time","race_time")\
    .withColumnRenamed("race_id","result_race_id")\
    .withColumnRenamed("file_date","result_file_date")


# COMMAND ----------

race_circuits_df=races_df.join(circuits_df,on="circuit_id",how="inner") \
    .select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

race_results=results_df.join(race_circuits_df,results_df.result_race_id==race_circuits_df.race_id,how="inner") \
    .join(drivers_df,on="driver_id",how="inner") \
    .join(constructors_df,on="constructor_id",how="inner")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df=race_results.select("race_id","race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position","result_file_date") \
    .withColumn("created_date",current_timestamp())\
    .withColumnRenamed("result_file_date","file_date")


# COMMAND ----------

display(final_df.filter("race_year==2020 and race_name=='Abu Dhabi Grand Prix'") \
    .orderBy("points",ascending=False))

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

merge_delta_data(final_df,'f1_presentation','race_results',presentation_folder_path,'tgt.driver_name = upd.driver_name AND tgt.race_id = upd.race_id','race_id')

# COMMAND ----------

# overwrite_partition(final_df,'f1_presentation','race_results',"race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results 

# COMMAND ----------

# %sql
# DROP TABLE f1_presentation.race_results

# COMMAND ----------

