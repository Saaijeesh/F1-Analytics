# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_filter=race_results_df.filter("race_year==2020")
display(demo_filter)

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct,sum

# COMMAND ----------

demo_filter.select(count("*")).show()

# COMMAND ----------

demo_filter.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_filter.select(sum("points")) \
    .withColumnRenamed("sum(points)", "total_points") \
    .show()

# COMMAND ----------

demo_filter.groupBy("driver_name").agg(sum("points"),countDistinct("race_name")).show()

# COMMAND ----------

demo_df=race_results_df.filter("race_year in (2019,2020)")
display(demo_df)

# COMMAND ----------

demo_grouped_df=demo_df.groupBy("race_year","driver_name").agg(sum("points"),countDistinct("race_name"))
display(demo_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

# COMMAND ----------

driverRankSpec=Window.partitionBy("race_year").orderBy(desc("sum(points)"))
demo_grouped_df.withColumn("rank",rank().over(driverRankSpec)).show()


# COMMAND ----------

