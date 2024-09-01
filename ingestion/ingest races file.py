# Databricks notebook source
# MAGIC %run "../includes/common functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text('p_file_date',"2021-03-21")
v_file_date= dbutils.widgets.get('p_file_date')

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,StructField,StructType,DateType

# COMMAND ----------

races_schema=StructType(fields=(StructField("raceId",IntegerType(),False), \
    StructField("year",IntegerType(),True), \
    StructField("round",IntegerType(),True), \
    StructField('circuitId',IntegerType(),True), \
    StructField("name",StringType(),True), \
    StructField("date",DateType(),True), \
    StructField("time",StringType(),True), \
    StructField("url",StringType(),True)))

# COMMAND ----------

races_df=spark.read.option("header",True) \
    .schema(races_schema) \
    .csv(f"dbfs:/mnt/formula1storageaccdl/raw/{v_file_date}/races.csv")
display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1storageaccdl/raw

# COMMAND ----------

from pyspark.sql.functions import col,lit,concat,to_timestamp,current_timestamp
races_mod_df=races_df.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss')) \
                     .withColumn("ingestion_date",current_timestamp())\
                    .withColumn("data_souce",lit(v_data_source))\
                    .withColumn("file_date",lit(v_file_date))
display(races_mod_df)

# COMMAND ----------

races_selected_df=races_mod_df.select(races_mod_df['raceId'],races_mod_df['year'],races_mod_df['round'],races_mod_df['circuitId'],races_mod_df['name'],races_mod_df['race_timestamp'],races_mod_df['ingestion_date'])
display(races_selected_df)

# COMMAND ----------

races_renamed_df=races_selected_df \
    .withColumnRenamed('raceId','race_id') \
    .withColumnRenamed('year','race_year') \
    .withColumnRenamed('circuitId','circuit_id')


# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

races_renamed_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1storageaccdl/processed/races

# COMMAND ----------

# df=spark.read.parquet("/mnt/formula1storageaccdl/processed/races")
# display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

