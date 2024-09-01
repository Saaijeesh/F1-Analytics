# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df=input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def rearrange_partition_col(input_df,partition_column):
    column_list=[]
    for name in input_df.schema.names:
        if name != partition_column:
            column_list.append(name)
    column_list.append(partition_column)

    output_df= input_df.select(column_list)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df,db_name,table,partition_column):
    output_df=rearrange_partition_col(input_df, partition_column)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table}"):  
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table}")
    else:
        #cutover data will be done with this
        output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table}")

# COMMAND ----------

def merge_delta_data(input_df,db_name,table,folder_path,merge_condition,partition_column):
    from delta.tables import DeltaTable
    if spark._jsparkSession.catalog().tableExists(f'{db_name}.{table}'):  
        deltaTable = DeltaTable.forPath(spark, f'{folder_path}/{table}')
        deltaTable.alias('tgt') \
    .merge(
        input_df.alias('upd'),
        merge_condition
    ) \
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
    else:
        #cutover data will be done with this
        input_df.write.mode("overwrite").partitionBy(f'{partition_column}').format("delta").saveAsTable(f'{db_name}.{table}')

# COMMAND ----------

