# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/asa/airlines

# COMMAND ----------

df = spark.read.format("csv").option("header",True).load("/databricks-datasets/asa/airlines")
df.display()

# COMMAND ----------

df.write.format("parquet").mode("overwrite").saveAsTable("flightdta1")

# COMMAND ----------


