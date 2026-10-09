# Databricks notebook source
# MAGIC %md
# MAGIC # CREATE FACT TABLE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading data

# COMMAND ----------

df_silver = spark.sql("""
                      SELECT * FROM parquet.`abfss://silver@salescar01.dfs.core.windows.net/carsales`
                      """)
display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading all dimention tables

# COMMAND ----------

df_model = spark.sql("SELECT * FROM cars_catalog.gold.dim_model")

df_dealer = spark.sql("SELECT * FROM cars_catalog.gold.dim_dealer")

df_date = spark.sql("SELECT * FROM cars_catalog.gold.dim_date")

df_branch = spark.sql("SELECT * FROM cars_catalog.gold.dim_branch")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Keys to Fact table with dimention tables

# COMMAND ----------

df_fact = df_silver.join(df_branch,df_silver['Branch_ID']==df_branch['Branch_ID'],how='left')\
    .join(df_dealer,df_silver['Dealer_ID']==df_dealer['Dealer_ID'],how='left')\
    .join(df_model,df_silver['Model_ID']==df_model['Model_ID'],how='left')\
    .join(df_date,df_silver['Date_ID']==df_date['Date_ID'],how='left')\
    .select(df_silver['Revenue'],df_silver['Units_Sold'],df_silver['RevPerUnit'],df_branch['dim_branch_key'],df_dealer
    ['dim_dealer_key'],df_model['dim_model_key'],df_date['dim_date_key'])

# COMMAND ----------

display(df_fact)

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing to Gold Fact Sales
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('factsales'):
    deltatbl = DeltaTable.forName(spark,'cars_catalog.gold.factsales')

    deltatbl.alias('trg').merge(df_fact.alias('src'),'trg.dim_branch_key = src.dim_branch_key and trg.dim_dealer_key = src.dim_dealer_key and trg.dim_model_key = src.dim_model_key and trg.dim_date_key = src.dim_date_key')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else :
    df_fact.write.format('delta')\
        .mode('Overwrite')\
        .option("path","abfss://gold@salescar01.dfs.core.windows.net/factsales")\
        .saveAsTable('cars_catalog.gold.factsales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.factsales