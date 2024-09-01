# Databricks notebook source
dbutils.widgets.text('p_file_date',"2021-03-28")
v_file_date= dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Find race year for which the data is to be preprocessed

# COMMAND ----------

race_results_list=spark.read.format('delta').load(f"{presentation_folder_path}/race_results")\
    .filter(f"file_date = '{v_file_date}'")\
    .select("race_year") \
    .distinct()\
    .collect()

# COMMAND ----------

race_year_list=[]
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

# COMMAND ----------

from pyspark.sql.functions import col
race_results_df=spark.read.format('delta').load(f"{presentation_folder_path}/race_results")\
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum,when,count,col
driver_standing_df= race_results_df \
  .groupBy("race_year","driver_name","driver_nationality") \
  .agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))




# COMMAND ----------

display(driver_standing_df.filter("race_year==2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank,desc

# COMMAND ----------

windowSpec=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))

# COMMAND ----------

final_df=driver_standing_df \
    .withColumn("rank", dense_rank().over(windowSpec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_rankings")

# COMMAND ----------

# overwrite_partition(final_df,'f1_presentation','driver_rankings',"race_year")

# COMMAND ----------

merge_delta_data(final_df,'f1_presentation','driver_rankings',presentation_folder_path,'tgt.driver_name = upd.driver_name AND tgt.race_year = upd.race_year','race_year')