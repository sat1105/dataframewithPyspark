# Databricks notebook source
from pyspark.sql.functions import to_date, col, year, month, dayofmonth, day
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from satish import notebook1

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


