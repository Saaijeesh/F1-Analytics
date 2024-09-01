# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common functions"

# COMMAND ----------

dbutils.widgets.text('p_file_date',"2021-03-28")
v_file_date= dbutils.widgets.get('p_file_date')

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

display(race_results_df.filter("race_year==2020"))

# COMMAND ----------

from pyspark.sql.functions import sum,desc,col,count,when
constructor_standing=race_results_df \
    .groupBy("race_year","team") \
    .agg(count(when(col("position")==1,True)).alias("wins"),sum("points").alias("total_points"))


    

# COMMAND ----------

display(constructor_standing)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank
windowSpec=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))

# COMMAND ----------

final_df=constructor_standing.withColumn("rank",rank().over(windowSpec))
display(final_df.filter("race_year=2020"))

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.construction_standings")

# COMMAND ----------

# overwrite_partition(final_df,'f1_presentation','construction_standings',"race_year")

# COMMAND ----------

merge_delta_data(final_df,'f1_presentation','construction_standings',presentation_folder_path,'tgt.team = upd.team AND tgt.race_year = upd.race_year','race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.construction_standings

# COMMAND ----------

