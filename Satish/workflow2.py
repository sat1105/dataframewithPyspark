# Databricks notebook source
from pyspark.sql.functions import to_date, col, year, month, dayofmonth
from pyspark.sql.types import *
from pyspark.sql import SparkSession

# COMMAND ----------

bronze = dbutils.widgets.text("bronze","dbfs:/mnt/s3-mount/Raw-bronze/")
silver = dbutils.widgets.text("silver","dbfs:/mnt/s3-mount/processed-silver/")
gold = dbutils.widgets.text("gold","dbfs:/mnt/s3-mount/aggregation-gold/")

# COMMAND ----------

df = (
    spark.read.format("csv").option("header", "true").option("nullValue","null")\
        .option("delimiter","\t").load(dbutils.widgets.get("bronze"))
)

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import to_date, col, year, month, day

def application(df):
    df_agg = df.withColumn("SALETIME", to_date(col("SALETIME"), "MM/dd/yyyy HH:mm:ss")) \
                .filter((col("PRICEPAID").isNotNull()) & (col("PRICEPAID") > 250) & (col("SALETIME").isNotNull())) \
                .withColumn("year", year("SALETIME")) \
                .withColumn("month", month("SALETIME")) \
                .withColumn("day", day("SALETIME"))
        
    display(df_agg)

# Example usage:
# transformed_df = application(original_df)

# COMMAND ----------

df_agg.write.format("parquet").mode("overwrite").partitionBy("year", "month", "day").save(dbutils.widgets.get("gold"))

# COMMAND ----------


