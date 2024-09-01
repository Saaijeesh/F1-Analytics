# Databricks notebook source
# MAGIC %run "../includes/common functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text('p_file_date',"2021-03-28")
v_file_date= dbutils.widgets.get('p_file_date')

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType, DateType,FloatType

# COMMAND ----------

results_schema=StructType(fields=[StructField("resultId",IntegerType(),False), 
                               StructField("raceId",IntegerType(),False), 
                               StructField("driverId",IntegerType(),False), 
                               StructField("constructorId",IntegerType(),False), 
                               StructField("number",IntegerType(),True),
                               StructField("grid",IntegerType(),False),
                               StructField("position",IntegerType(),True),
                               StructField("positionText",StringType(),False),
                               StructField("positionOrder",IntegerType(),False),
                               StructField("points",FloatType(),False),
                               StructField("laps",IntegerType(),False),
                               StructField("time",StringType(),True),
                               StructField("milliseconds",IntegerType(),True),
                               StructField("fastestLap",IntegerType(),True),
                               StructField("rank",IntegerType(),True),
                               StructField("fastestLapTime",StringType(),True),
                               StructField("fastestLapSpeed",StringType(),True),
                               StructField("statusId",IntegerType(),False)
                                   ])

# COMMAND ----------

results_df=spark.read.schema(results_schema).json(f'/mnt/formula1storageaccdl/raw/{v_file_date}/results.json')
display(results_df)

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,col,concat
results_mod_df=results_df.withColumnRenamed("resultId","result_id") \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("constructorId","constructor_id") \
    .withColumnRenamed("positionText","position_text") \
    .withColumnRenamed("positionOrder","position_order") \
    .withColumnRenamed("fastestLap","fastest_lap") \
    .withColumnRenamed("fastestLapTime","fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("data_source",lit(v_data_source))\
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(results_mod_df)

# COMMAND ----------

results_final_df=results_mod_df.drop(results_mod_df['statusId'])
display(results_final_df)

# COMMAND ----------

results_final_df.columns

# COMMAND ----------

results_deduped_df=results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if spark._jsparkSession.catalog().tableExists("f1_processed.results"):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 2

# COMMAND ----------

# def rearrange_partition_col(input_df,partition_column):
#     column_list=[]
#     for name in input_df.schema.names:
#         if name != partition_column:
#             column_list.append(name)
#     column_list.append(partition_column)

#     output_df= input_df.select(column_list)
#     return output_df

# COMMAND ----------

# def overwrite_partition(input_df,db_name,table,partition_column):
#     output_df=rearrange_partition_col(input_df, partition_column)

#     spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
#     if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table}"):  
#         output_df.write.mode("overwrite").insertInto(f"{db_name}.{table}")
#     else:
#         #cutover data will be done with this
#         output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table}")


# COMMAND ----------

# overwrite_partition(results_final_df,'f1_processed','results',"race_id")

# COMMAND ----------

# if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table}"):  
#     output_df.write.mode("overwrite").insertInto(f"{db_name}.{table}")
# else:
#     #cutover data will be done with this
#     output_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table}")

# COMMAND ----------

merge_delta_data(results_deduped_df,'f1_processed','results',processed_folder_path,'tgt.result_id = upd.result_id AND tgt.race_id = upd.race_id','race_id')

# COMMAND ----------

# from delta.tables import DeltaTable
# if spark._jsparkSession.catalog().tableExists('f1_processed.results'):  
#     deltaTable = DeltaTable.forPath(spark, '/mnt/formula1storageaccdl/processed/results')
#     deltaTable.alias('tgt') \
#   .merge(
#     results_final_df.alias('upd'),
#     'tgt.result_id = upd.result_id AND tgt.race_id = upd.race_id'
#   ) \
#   .whenMatchedUpdateAll()\
#   .whenNotMatchedInsertAll()\
#   .execute()
# else:
#     #cutover data will be done with this
#     results_final_df.write.mode("overwrite").partitionBy('race_id').format("delta").saveAsTable('f1_processed.results')

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
# results_final_df = results_final_df.select(
#     "result_id",
#     "driver_id",
#     "constructor_id",
#     "number",
#     "grid",
#     "position",
#     "position_text",
#     "position_order",
#     "points",
#     "laps",
#     "time",
#     "milliseconds",
#     "fastest_lap",
#     "rank",
#     "fastest_lap_time",
#     "fastest_lap_speed",
#     "ingestion_date",
#     "data_source",
#     "file_date",
#     "race_id"  # race_id as the last column
# )


# COMMAND ----------

# ##this will consider last column as the partition by column.
# ##In this case, we will have to change race_id to the last column
# #adding the data to the table next time after cutover can be done with this

# if spark._jsparkSession.catalog().tableExists("f1_processed.results"):  
#     results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     #cutover data will be done with this
#     results_final_df.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")


# COMMAND ----------

# df=spark.read.parquet('/mnt/formula1storageaccdl/processed/results')
# display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# %sql
# SELECT DISTINCT race_id,COUNT(1) FROM f1_processed.results
# WHERE race_id == 1053
# GROUP BY race_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT file_date,COUNT(1) FROM f1_processed.results
# MAGIC GROUP BY file_date

# COMMAND ----------

# results_final_df.select("race_id").distinct()

# COMMAND ----------

# %sql
# select distinct race_id, COUNT(1) FROM f1_processed.results
# GROUP by race_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE f1_processed.results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,driver_id,count(1) 
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id,driver_id
# MAGIC HAVING count(1)>1
# MAGIC ORDER BY race_id,driver_id DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  f1_processed.results WHERE race_id =540 AND driver_id=229

# COMMAND ----------

# %sql
# DROP TABLE f1_processed.results

# COMMAND ----------

