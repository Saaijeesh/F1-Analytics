# Databricks notebook source
# MAGIC %sql
# MAGIC USE f1_processed

# COMMAND ----------

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

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results 
            (
            race_year INT,
            team_name STRING,
            driver_id INT,
            driver_name STRING,
            race_id INT,
            position INT,
            points INT,
            calculated_points INT,
            created_date TIMESTAMP,
            updated_date TIMESTAMP
            )
            using DELTA
        """)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW race_result_update AS 
SELECT 
races.race_year,
constructors.name as team_name,
drivers.driver_id,
drivers.name as driver_name,
races.race_id,
results.position,
results.points ,
11 - results.position as calculated_points FROM f1_processed.results
INNER JOIN drivers ON results.driver_id = drivers.driver_id
INNER JOIN constructors ON results.constructor_id = constructors.constructor_id
INNER JOIN races ON results.race_id = races.race_id
WHERE results.position <=10 AND results.file_date = '{v_file_date}'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM race_result_update

# COMMAND ----------

spark.sql(f"""
MERGE INTO f1_presentation.calculated_race_results tgt
USING race_result_update upd
ON tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id
WHEN MATCHED THEN
  UPDATE SET
    tgt.position = upd.position,
    tgt.points = upd.points,
    tgt.calculated_points = upd.calculated_points,
    tgt.updated_date = current_timestamp
    
WHEN NOT MATCHED
  THEN INSERT (
    race_year,
    team_name,
    driver_id,
    driver_name,
    race_id,
    position,
    points,
    calculated_points,
    created_date

  )
  VALUES (
    race_year,
    team_name,
    driver_id,
    driver_name,
    race_id,
    position,
    points,
    calculated_points,
    current_timestamp
  )""")

# COMMAND ----------

# %sql
# CREATE TABLE f1_presentation.calculated_race_results 
# USING PARQUET AS
# SELECT 
# races.race_year,
# constructors.name as team_name,
# drivers.name as driver_name,
# results.position,
# results.points ,
# 11 - results.position as calculated_points FROM results
# INNER JOIN drivers ON results.driver_id = drivers.driver_id
# INNER JOIN constructors ON results.constructor_id = constructors.constructor_id
# INNER JOIN races ON results.race_id = races.race_id
# WHERE results.position <=10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM race_result_update

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM f1_presentation.calculated_race_results 

# COMMAND ----------

# %sql
# DROP TABLE f1_presentation.calculated_race_results

# COMMAND ----------

