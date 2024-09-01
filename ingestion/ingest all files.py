# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result=dbutils.notebook.run("ingest circuit file",0,{"p_data_source":"Ergast API","p_file_date":"2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("ingest constructors file",0,{"p_data_source":"Ergast API","p_file_date":"2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("ingest drivers file",0,{"p_data_source":"Ergast API","p_file_date":"2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("ingest laptime file",0,{"p_data_source":"Ergast API","p_file_date":"2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("ingest pitstops file",0,{"p_data_source":"Ergast API","p_file_date":"2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("ingest qualifying file",0,{"p_data_source":"Ergast API","p_file_date":"2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("ingest races file",0,{"p_data_source":"Ergast API","p_file_date":"2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("ingest results file",0,{"p_data_source":"Ergast API","p_file_date":"2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

