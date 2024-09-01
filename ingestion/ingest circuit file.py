# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuit file

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text('p_data_source',"")
v_data_source= dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date',"2021-03-21")
v_file_date= dbutils.widgets.get('p_file_date')

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1- Read csv file using spark dataframe reader

# COMMAND ----------

# MAGIC %md
# MAGIC When we add inferschema option, it increases the number of spark jobs to 2 which means it takes it read the file. In real time, we would work on a big data and would take a huge amount of time to read the data as it checks the data one by one to give the correct datatype. Instead we should set the schema prior and use.

# COMMAND ----------

# circuits_df=spark.read \
#     .option("header",True) \
#     .option("inferSchema",True) \
#     .csv("dbfs:/mnt/formula1storageaccdl/raw/circuits.csv")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

circuits_schema= StructType(fields=[StructField("circuitId",IntegerType(),False),
                                    StructField("circuitRef",StringType(),True),
                                    StructField("name",StringType(),True),
                                    StructField("location",StringType(),True),
                                    StructField("country",StringType(),True),
                                    StructField("lat",DoubleType(),True),
                                    StructField("lng",DoubleType(),True),
                                    StructField("alt",IntegerType(),True),
                                    StructField("url",StringType(),True)

])

# COMMAND ----------

circuits_df=spark.read \
    .option("header",True) \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC To know location of the file

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1storageaccdl/raw

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

circuits_df.show(5)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select the required columns

# COMMAND ----------

# circuits_selected_df=circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

#can also be written like this

# circuits_selected_df=circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lng,circuits_df.alt)

circuits_selected_df=circuits_df.select(circuits_df["circuitId"],circuits_df["circuitRef"].alias('circuit_ref'),circuits_df["name"],circuits_df["location"],circuits_df["country"],circuits_df["lat"].alias('latitude'),circuits_df["lng"].alias('longitude'),circuits_df["alt"].alias('altitude'))

# COMMAND ----------

# from pyspark.sql.functions import col
# circuits_selected_df=circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename the column names

# COMMAND ----------

circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitId","circuit_id")

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

# circuits_final_df=circuits_renamed_df.withColumn("ingestion_date",current_timestamp()) \
#     .withColumn("env",lit("Production")) #add literal to all rows

# COMMAND ----------

circuits_final_df=add_ingestion_date(circuits_renamed_df)\
    .withColumn("data_source",lit(v_data_source))\
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to data lake as parquet file

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").parquet("f{processed_folder_path}/circuits")

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1storageaccdl/processed/circuits

# COMMAND ----------

# df=spark.read.delta("/mnt/formula1storageaccdl/processed/circuits")
# display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

