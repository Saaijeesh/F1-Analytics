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

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType, DateType

# COMMAND ----------

pitstop_schema=StructType(fields=[StructField("raceId",IntegerType(),False), 
                               StructField("driverId",IntegerType(),False), 
                               StructField("stop",IntegerType(),False), 
                               StructField("lap",IntegerType(),False), 
                               StructField("time",StringType(),False),
                               StructField("duration",StringType(),True),
                               StructField("milliseconds",IntegerType(),True)
                                   ])

# COMMAND ----------

pitstop_df=spark.read.schema(pitstop_schema) \
    .option("multiline",True).json(f'/mnt/formula1storageaccdl/raw/{v_file_date}/pit_stops.json')
display(pitstop_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,col,concat
pitstop_mod_df=pitstop_df.withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("data_source",lit(v_data_source))\
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

merge_delta_data(pitstop_mod_df,'f1_processed','pitstop',processed_folder_path,'tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id AND tgt.stop = upd.stop','race_id')

# COMMAND ----------

# overwrite_partition(pitstop_mod_df,'f1_processed','pitstop',"race_id")

# COMMAND ----------

# pitstop_mod_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pitstop")

# COMMAND ----------

# df=spark.read.parquet('/mnt/formula1storageaccdl/processed/pitstop')
# display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT file_date,count(1) FROM f1_processed.pitstop
# MAGIC GROUP BY file_date

# COMMAND ----------

