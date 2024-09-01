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

laptime_schema=StructType(fields=[StructField("raceId",IntegerType(),False), 
                               StructField("driverId",IntegerType(),False),  
                               StructField("lap",IntegerType(),False),
                               StructField("position",IntegerType(),True), 
                               StructField("time",StringType(),True),
                               StructField("milliseconds",IntegerType(),True)
                                   ])

# COMMAND ----------

laptime_df=spark.read.schema(laptime_schema) \
    .csv(f'/mnt/formula1storageaccdl/raw/{v_file_date}/lap_times/lap_times_split*.csv')
display(laptime_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,col,concat
laptime_mod_df=laptime_df.withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("data_source",lit(v_data_source))\
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(laptime_mod_df)

# COMMAND ----------

# laptime_mod_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.laptime")

# COMMAND ----------

merge_delta_data(laptime_mod_df,'f1_processed','laptime',processed_folder_path,'tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id AND tgt.lap = upd.lap','race_id')

# COMMAND ----------

laptime_mod_df.printSchema()

# COMMAND ----------

# df=spark.read.parquet('/mnt/formula1storageaccdl/processed/laptime')
# display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT file_date,COUNT(1) FROM f1_processed.laptime
# MAGIC GROUP BY file_date

# COMMAND ----------

