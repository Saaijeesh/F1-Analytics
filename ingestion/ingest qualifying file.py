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

qualify_schema=StructType(fields=[StructField("qualifyId",IntegerType(),False),
                               StructField("raceId",IntegerType(),False), 
                               StructField("driverId",IntegerType(),False),  
                               StructField("constructorId",IntegerType(),False),
                               StructField("number",IntegerType(),True), 
                               StructField("position",IntegerType(),True),
                               StructField("q1",StringType(),True),
                               StructField("q2",StringType(),True),
                               StructField("q3",StringType(),True)
                                   ])

# COMMAND ----------

qualify_df=spark.read.schema(qualify_schema) \
    .option('multiLine',True) \
    .json(f'/mnt/formula1storageaccdl/raw/{v_file_date}/qualifying')
display(qualify_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,col,concat
qualify_mod_df=qualify_df.withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("constructorId","constructor_id") \
    .withColumnRenamed("qualifyId","qualify_id") \
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("data_source",lit(v_data_source))\
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

merge_delta_data(qualify_mod_df,'f1_processed','qualifying',processed_folder_path,'tgt.qualify_id = upd.qualify_id','race_id')

# COMMAND ----------

# overwrite_partition(qualify_mod_df,'f1_processed','qualifying',"race_id")

# COMMAND ----------

# qualify_mod_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# df=spark.read.parquet('/mnt/formula1storageaccdl/processed/qualifying')
# display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT file_date,COUNT(1) FROM f1_processed.qualifying
# MAGIC GROUP BY file_date

# COMMAND ----------

