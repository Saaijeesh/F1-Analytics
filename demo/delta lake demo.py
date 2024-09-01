# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE f1_demo
# MAGIC LOCATION '/mnt/formula1storageaccdl/demo'

# COMMAND ----------

results_df=spark.read \
.option('inferSchema',True)\
.json('/mnt/formula1storageaccdl/raw/2021-03-28/results.json')

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').saveAsTable('f1_demo.results_managed')

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').save('/mnt/formula1storageaccdl/demo/results_external')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formula1storageaccdl/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM F1_DEMO.results_external

# COMMAND ----------

results_external_df=spark.read.format('delta').load('/mnt/formula1storageaccdl/demo/results_external')
display(results_external_df)

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').partitionBy('constructorId').saveAsTable('f1_demo.results_partition')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partition

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update and Delete in Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed SET points = 11 - position WHERE position <=10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1storageaccdl/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "position <=10",
  set = { "points": "21 - position" }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed WHERE position>10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1storageaccdl/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete("position > 8")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert using merge

# COMMAND ----------

driver_day1_df=spark.read \
  .option("inferSchema",True) \
  .json('/mnt/formula1storageaccdl/raw/2021-03-28/drivers.json') \
  .filter("driverId <=10")\
  .select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

display(driver_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper

driver_day2_df=spark.read \
  .option("inferSchema",True) \
  .json('/mnt/formula1storageaccdl/raw/2021-03-28/drivers.json') \
  .filter("driverId BETWEEN 6 AND 15")\
  .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

display(driver_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper

driver_day3_df=spark.read \
  .option("inferSchema",True) \
  .json('/mnt/formula1storageaccdl/raw/2021-03-28/drivers.json') \
  .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20 ")\
  .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

display(driver_day3_df)

# COMMAND ----------

driver_day1_df.createTempView("driver_day1")

# COMMAND ----------

driver_day2_df.createTempView("driver_day2")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS F1_demo.drivers_merge (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## DAY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO F1_demo.drivers_merge tgt
# MAGIC USING driver_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updatedDate = current_timestamp
# MAGIC     
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     upd.driverId,
# MAGIC     upd.dob,
# MAGIC     upd.forename,
# MAGIC     upd.surname,
# MAGIC     current_timestamp
# MAGIC     
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM F1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ## DAY 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO F1_demo.drivers_merge tgt
# MAGIC USING driver_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updatedDate = current_timestamp
# MAGIC     
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     upd.driverId,
# MAGIC     upd.dob,
# MAGIC     upd.forename,
# MAGIC     upd.surname,
# MAGIC     current_timestamp
# MAGIC     
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM F1_demo.drivers_merge

# COMMAND ----------

from delta.tables import DeltaTable

deltaTablePeople = DeltaTable.forPath(spark, '/mnt/formula1storageaccdl/demo/drivers_merge')
# deltaTablePeopleUpdates = DeltaTable.forPath(spark, '/tmp/delta/people-10m-updates')

# dfUpdates = deltaTablePeopleUpdates.toDF()

deltaTablePeople.alias('tgt') \
  .merge(
    driver_day3_df.alias('upd'),
    'tgt.driverId = upd.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "tgt.dob" : "upd.dob",
    "tgt.forename" : "upd.forename",
    "tgt.surname" : "upd.surname",
    "tgt.updatedDate" : "current_timestamp()"
      
    }
  ) \
  .whenNotMatchedInsert(values =
    {

        "driverId": "upd.driverId",
        "dob":"upd.dob",
        "forename":"upd.forename",
        "surname":"upd.surname",
        "createdDate":"current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM F1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-07-04T07:46:37.000+00:00'

# COMMAND ----------

df=spark.read.format('delta').option("timestampAsOf",'2024-07-04T07:46:37.000+00:00').load('/mnt/formula1storageaccdl/demo/drivers_merge')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-07-04T07:46:37.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-07-04T07:46:37.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge WHERE driverId =6

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 9

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge AS tgt 
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 9 src
# MAGIC ON tgt.driverId = src.driverId
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transaction log

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS F1_demo.drivers_txn (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY F1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO F1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge WHERE driverId =1 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY F1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO F1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge WHERE driverId =2

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM F1_demo.drivers_txn
# MAGIC WHERE driverId =2

# COMMAND ----------

for driver_id in range(3,20):
    spark.sql(f"""INSERT INTO F1_demo.drivers_txn
                SELECT * FROM f1_demo.drivers_merge WHERE driverId ={driver_id}
              """)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO F1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ##Convert parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS F1_demo.drivers_convert_to_delta (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO F1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA F1_demo.drivers_convert_to_delta

# COMMAND ----------

df=spark.table("F1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save('/mnt/formula1storageaccdl/demo/drivers_convert_to_delta_new')

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/formula1storageaccdl/demo/drivers_convert_to_delta_new`

# COMMAND ----------

