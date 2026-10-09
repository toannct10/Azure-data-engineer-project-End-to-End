# Databricks notebook source
# MAGIC %md
# MAGIC # Reading data

# COMMAND ----------

df = spark.read.format('parquet')\
                .option('inferSchema',True)\
                .load('abfss://bronze@salescar01.dfs.core.windows.net/rawdata')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df= df.withColumn('model_category', split(col('Model_ID'),'-')[0])
display(df)

# COMMAND ----------

df = df.withColumn('RevPerUnit', col('Revenue')/col('Units_Sold'))
display(df)

# COMMAND ----------

df_total_units =df.groupBy('Year','BranchName').agg(sum('Units_Sold').alias('Total_Units')).sort('Year', 'Total_Units', ascending=[1,0])
display(df_total_units)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing

# COMMAND ----------

df.write.format('parquet')\
    .mode('append')\
    .option('path','abfss://silver@salescar01.dfs.core.windows.net/carsales')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Query silver data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM parquet.`abfss://silver@salescar01.dfs.core.windows.net/carsales`