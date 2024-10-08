-- Databricks notebook source
CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results=spark.read.parquet(f"{presentation_folder_path}/race_results")
-- MAGIC display(race_results)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

DESCRIBE race_results_python

-- COMMAND ----------

DESCRIBE EXTENDED race_results_python

-- COMMAND ----------

SELECT * FROM demo.race_results_python

-- COMMAND ----------

CREATE TABLE demo.race_results_sql AS
SELECT * FROM demo.race_results_python
WHERE race_year ==2020

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

SELECT * FROM demo.race_results_sql

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## EXTERNAL TABLES

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

DESCRIBE EXTENDED race_results_ext_py

-- COMMAND ----------

CREATE TABLE race_results_ext_sql 
(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING PARQUET
LOCATION "/mnt/formula1storageaccdl/presentation/race_results_ext_sql"

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql 
SELECT * FROM demo.race_results_ext_py WHERE race_year ==2020 


-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

DROP TABLE race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##VIEW

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

CREATE TEMP VIEW v_race_results 
 AS SELECT * FROM demo.race_results_python
 WHERE race_year=2020

-- COMMAND ----------

SELECT * FROM v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW Gv_race_results
 AS SELECT * FROM demo.race_results_python
 WHERE race_year=2018


-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

SHOW TABLES IN global_temp

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
 AS SELECT * FROM demo.race_results_python
 WHERE race_year=2012

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

