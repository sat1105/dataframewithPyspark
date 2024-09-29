# Databricks notebook source
import sys
import os
sys.path.append('../')
import satish.src.utils as ut 

# COMMAND ----------

import sys
import os

repo_root = '/Workspace/Repos/workflows/dataframewithPyspark'
sys.path.append(os.path.abspath(repo_root))

# Now you can import as before
import satish.src.utils as nb1

# COMMAND ----------

import sys

# Assuming your module is uploaded to DBFS in the directory '/dbfs/FileStore/my_modules'
module_path = '/dbfs/FileStore/my_modules'
if module_path not in sys.path:
    sys.path.append(module_path)

from satish.src.utils import demo2

# COMMAND ----------

bronze = dbutils.widgets.text("bronze","dbfs:/mnt/s3-mount/Raw-bronze/")
silver = dbutils.widgets.text("silver","dbfs:/mnt/s3-mount/processed-silver/")
gold = dbutils.widgets.text("gold","dbfs:/mnt/s3-mount/aggregation-gold/")

# COMMAND ----------

schema = StructType([StructField('SALESID', IntegerType(), True), 
                     StructField('LISTID', IntegerType(), True),
                     StructField('SELLERID', IntegerType(), True), 
                     StructField('BUYERID', IntegerType(), True), 
                     StructField('EVENTID', IntegerType(), True), 
                     StructField('DATEID', IntegerType(), True), 
                     StructField('QTYSOLD', IntegerType(), True), 
                     StructField('PRICEPAID', IntegerType(), True), 
                     StructField('COMMISSION', IntegerType(), True), 
                     StructField('SALETIME', StringType(), True)]
                    )

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").option("delimiter","\t").option("nullValue","null").schema(schema).load(dbutils.widgets.get("silver"))

# COMMAND ----------

df1_agg = df1.withColumn("SALETIME", to_date(col("SALETIME"), "MM/dd/yyyy HH:mm:ss")) \
            .filter((col("PRICEPAID").isNotNull()) & (col("PRICEPAID") > 250) & (col("SALETIME").isNotNull())) \
            .withColumn("year", year("SALETIME")) \
            .withColumn("month", month("SALETIME")) \
            .withColumn("day", dayofmonth("SALETIME"))
df1_agg.write.format("parquet").mode("overwrite").partitionBy("year", "month", "day").save(dbutils.widgets.get("gold"))


# COMMAND ----------


