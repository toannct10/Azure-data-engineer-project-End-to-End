# Databricks notebook source
# MAGIC %md
# MAGIC # CREATE FLAG PARAMETER

# COMMAND ----------

dbutils.widgets.text('incremental', '0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental')
print(incremental_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Dimention Model

# COMMAND ----------

df_src = spark.sql("""
        SELECT distinct(Date_ID) as Date_ID
        FROM parquet.`abfss://silver@salescar01.dfs.core.windows.net/carsales`
        """)

# COMMAND ----------

display(df_src)

# COMMAND ----------

# MAGIC %md
# MAGIC #Dim Sink - Initial - Incremetal Load

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
    df_sink = spark.sql("""
            select dim_date_key, Date_ID from cars_catalog.gold.dim_date
            """)
else:
    df_sink = spark.sql("""
            select 1 as dim_date_key, Date_ID from parquet.`abfss://silver@salescar01.dfs.core.windows.net/carsales`
            where 1=0
            """)

# COMMAND ----------

display(df_sink)

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.Date_ID==df_sink.Date_ID, 'left').select(df_src['Date_Id'],
                                                                                   df_sink['dim_date_key'])
display(df_filter)

# COMMAND ----------

df_old = df_filter.filter(df_filter.dim_date_key.isNotNull())
df_new = df_filter.filter(df_filter.dim_date_key.isNull())
display(df_old)

# COMMAND ----------

display(df_new)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

if incremental_flag == '0':
    max_value = 1
else:
    max_value_df = spark.sql("SELECT max(dim_date_key) from cars_catalog.gold.dim_date")
    max_value = max_value_df.collect()[0][0]+1
df_new = df_new.withColumn('dim_date_key',max_value+monotonically_increasing_id())

# COMMAND ----------

df_new.display()

# COMMAND ----------

df_final = df_new.union(df_old)
df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing to Gold -SCD-TYPE 1

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

#Incremental Run
if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
    delta_tbl = DeltaTable.forPath(spark, "abfss://gold@salescar01.dfs.core.windows.net/dim_date")
    delta_tbl.alias('sink').merge(df_final.alias('src'), "sink.dim_date_key = src.dim_date_key")\
                        .whenMatchedUpdateAll()\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    df_final.write.format('delta')\
        .mode('overwrite')\
        .option("path", "abfss://gold@salescar01.dfs.core.windows.net/dim_date")\
        .saveAsTable('cars_catalog.gold.dim_date')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_date