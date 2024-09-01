# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text('p_file_date',"2021-03-21")
v_file_date= dbutils.widgets.get('p_file_date')

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType, DateType

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename",StringType(),True), \
                               StructField("surname",StringType(),True)])

# COMMAND ----------

driver_schema=StructType(fields=[StructField("driverId",IntegerType(),False), 
                               StructField("driverRef",StringType(),True), 
                               StructField("number",IntegerType(),True), 
                               StructField("code",StringType(),True), 
                               StructField("name",name_schema),
                               StructField("dob",DateType(),True),
                               StructField("nationality",StringType(),True),
                               StructField("url",StringType(),True)
                                   ])

# COMMAND ----------

drivers_df=spark.read.schema(driver_schema).json(f'/mnt/formula1storageaccdl/raw/{v_file_date}/drivers.json')
display(drivers_df)

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,col,concat
drivers_mod_df=drivers_df.withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("driverRef","driver_ref") \
    .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname"))) \
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("data_souce",lit(v_data_source))\
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

drivers_final_df=drivers_mod_df.drop(drivers_mod_df['url'])
display(drivers_final_df)

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# df=spark.read.parquet('/mnt/formula1storageaccdl/processed/drivers')
# display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

