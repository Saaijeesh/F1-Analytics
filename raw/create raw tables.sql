-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits
(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
) USING CSV
OPTIONS (path "/mnt/formula1storageaccdl/raw/circuits.csv",header true)


-- COMMAND ----------

SELECT * FROM f1_raw.circuits

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races
(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
) USING CSV
OPTIONS (path "/mnt/formula1storageaccdl/raw/races.csv")


-- COMMAND ----------

SELECT * FROM f1_raw.races

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors
(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING

) USING JSON
OPTIONS (path "/mnt/formula1storageaccdl/raw/constructors.json")


-- COMMAND ----------

SELECT * FROM f1_raw.constructors

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers
(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: string,surname: string>,
dob DATE,
nationality STRING,
url STRING
) USING JSON
OPTIONS (path "/mnt/formula1storageaccdl/raw/drivers.json")


-- COMMAND ----------

SELECT * FROM f1_raw.drivers

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results
(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points FLOAT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed STRING,
statusId INT

) USING JSON
OPTIONS (path "/mnt/formula1storageaccdl/raw/results.json")


-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pitstops;
CREATE TABLE IF NOT EXISTS f1_raw.pitstops
(
raceId INT,
driverId INT,
stop INT,
lap INT,
time STRING,
duration STRING,
milliseconds INT

) USING JSON
OPTIONS (path "/mnt/formula1storageaccdl/raw/pit_stops.json",multiLine true)


-- COMMAND ----------

SELECT * FROM f1_raw.pitstops

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.laptime;
CREATE TABLE IF NOT EXISTS f1_raw.laptime
(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT

) USING CSV
OPTIONS (path "/mnt/formula1storageaccdl/raw/lap_times")


-- COMMAND ----------

SELECT * FROM f1_raw.laptime

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying
(
qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING

) USING json
OPTIONS (path "/mnt/formula1storageaccdl/raw/qualifying",multiLine true)


-- COMMAND ----------

SELECT * FROM f1_raw.qualifying

-- COMMAND ----------

